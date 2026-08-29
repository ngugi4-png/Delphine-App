/**
 * Delphine Enterprise — Inventory Sync Cloud Function
 * -----------------------------------------------------
 * Triggers on every new document in `transactions/{txnId}`.
 *   type === 'purchase'  -> increase inventory
 *   type === 'sale'      -> decrease inventory
 *
 * Guarantees:
 *   - Atomic read-modify-write via a single Firestore transaction
 *     (equivalent to row-level locking — Firestore locks the docs
 *     it reads inside the transaction until commit).
 *   - Idempotent: Cloud Functions triggers are at-least-once, so the
 *     same txnId can fire more than once. A ledger doc keyed by
 *     txnId is checked *inside* the transaction; if it already
 *     exists we no-op instead of double-applying the change.
 *   - Negative stock is allowed: a sale that exceeds what's on hand
 *     still applies, leaving qty negative (a visible deficit) rather
 *     than being rejected. Every change still writes one audit row.
 *
 * Expected shape of a document in `transactions`:
 *   {
 *     type:    'purchase' | 'sale',
 *     branch:  'Kahuti' | 'Karega' | 'Kanyenyaini',
 *     product: 'Petrol (PMS)' | '2T Motor Oil' | ... ,
 *     qty:     number   (always positive; direction comes from `type`),
 *     unit:    'L' | 'pcs' (optional, stored on the inventory doc),
 *     actor:   string (optional — email/uid of who recorded it)
 *   }
 */

const { onDocumentCreated } = require('firebase-functions/v2/firestore');
const { onSchedule } = require('firebase-functions/v2/scheduler');
const { logger } = require('firebase-functions');
const admin = require('firebase-admin');
const {
  notifyAdmins,
  AT_USERNAME, AT_API_KEY, WA_PHONE_NUMBER_ID, WA_ACCESS_TOKEN,
} = require('./lib/notify');

admin.initializeApp();
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;

// Same doc-id scheme the web app already uses for inventory docs,
// so this function updates the exact same records the UI reads.
function inventoryDocId(branch, product) {
  return `${branch}_${product}`.replace(/[^a-z0-9]/gi, '_');
}

exports.syncInventoryOnTransaction = onDocumentCreated(
  'transactions/{txnId}',
  async (event) => {
    const txnId = event.params.txnId;
    const snap = event.data;
    if (!snap) {
      logger.warn(`No data on transaction ${txnId}`);
      return;
    }
    const txn = snap.data();

    const { type, branch, product, qty } = txn;

    // ── Basic validation ────────────────────────────────────────
    if (type !== 'purchase' && type !== 'sale') {
      logger.warn(`Transaction ${txnId} has unknown type "${type}" — skipped`);
      return;
    }
    if (!branch || !product || typeof qty !== 'number' || qty <= 0) {
      logger.warn(`Transaction ${txnId} missing/invalid branch, product, or qty — skipped`);
      await snap.ref.set(
        { status: 'rejected', rejectReason: 'invalid_fields', processedAt: FieldValue.serverTimestamp() },
        { merge: true }
      );
      return;
    }

    const invRef    = db.collection('inventory').doc(inventoryDocId(branch, product));
    const ledgerRef = db.collection('inventoryLedger').doc(txnId); // idempotency guard
    const auditRef  = db.collection('inventoryAuditLog').doc();    // one row per applied/rejected change

    try {
      await db.runTransaction(async (tx) => {
        // ── ALL reads must happen before any writes in a Firestore transaction ──
        const ledgerSnap = await tx.get(ledgerRef);
        if (ledgerSnap.exists) {
          // Already processed (function retried / duplicate delivery). No-op.
          logger.info(`Transaction ${txnId} already applied — idempotent skip`);
          return;
        }

        const invSnap = await tx.get(invRef);
        const currentQty = invSnap.exists ? (invSnap.data().qty || 0) : 0;

        const delta  = type === 'purchase' ? qty : -qty;
        const newQty = currentQty + delta;
        // Negative stock is allowed (e.g. sales recorded before a delayed
        // purchase/delivery is entered). A deficit just means newQty < 0 —
        // it still gets applied and audited like any other change.

        // ── Apply the stock change ─────────────────────────────
        tx.set(
          invRef,
          {
            branch,
            product,
            qty: newQty,
            unit: txn.unit || (invSnap.exists ? invSnap.data().unit : null) || null,
            updatedAt: FieldValue.serverTimestamp(),
            lastTxnId: txnId,
          },
          { merge: true }
        );

        // ── Idempotency ledger — marks this txnId as consumed ──
        tx.set(ledgerRef, {
          txnId, type, branch, product, qty,
          status: 'applied',
          qtyBefore: currentQty,
          qtyAfter: newQty,
          deficit: newQty < 0,
          appliedAt: FieldValue.serverTimestamp(),
        });

        // ── Audit log — one immutable row per change ───────────
        tx.set(auditRef, {
          txnId, branch, product, type,
          qtyRequested: qty,
          qtyBefore: currentQty,
          qtyAfter: newQty,
          delta,
          status: 'applied',
          deficit: newQty < 0,   // flags a negative-stock event without blocking it
          actor: txn.actor || null,
          timestamp: FieldValue.serverTimestamp(),
        });

        // ── Mark the source transaction as processed ───────────
        tx.set(
          snap.ref,
          { status: 'applied', processedAt: FieldValue.serverTimestamp() },
          { merge: true }
        );
      });

      logger.info(`Transaction ${txnId} (${type} ${qty} ${product} @ ${branch}) processed`);
    } catch (err) {
      logger.error(`Failed to process transaction ${txnId}:`, err);
      // Leave the source doc as-is (no status written) so it's easy to
      // find un-processed transactions and re-drive them if needed.
      throw err; // let Cloud Functions retry per its own retry policy
    }
  }
);

const BRANCHES = ['Kahuti', 'Karega', 'Kanyenyaini'];
const SECRETS = [AT_USERNAME, AT_API_KEY, WA_PHONE_NUMBER_ID, WA_ACCESS_TOKEN];

/**
 * Anomaly Alerts — Sales Reports
 * -------------------------------
 * Fires on every new report. Flags:
 *   - litres sold recorded but revenue is zero (likely data-entry error)
 *   - revenue far above or below the branch's own 14-day trailing average
 * Thresholds are read from `settings/alerts` so they're editable in-app
 * without a redeploy. Sends via whatever channels are configured.
 */
exports.checkReportAnomaly = onDocumentCreated(
  { document: 'reports/{reportId}', secrets: SECRETS },
  async (event) => {
    const snap = event.data;
    if (!snap) return;
    const r = snap.data();
    const branch = r.branch;
    if (!branch) return;

    const revenue = r.grandTotal || 0;
    const litres = (r.pump && r.pump.metre && r.pump.metre.sold) || 0;

    const cfgSnap = await db.collection('settings').doc('alerts').get();
    const cfg = cfgSnap.exists ? cfgSnap.data() : {};
    const multiplier = cfg.reportAnomalyMultiplier || 2.5;

    const since = new Date();
    since.setDate(since.getDate() - 14);
    const sinceStr = since.toISOString().split('T')[0];

    const histSnap = await db.collection('reports')
      .where('branch', '==', branch)
      .where('date', '>=', sinceStr)
      .get();

    let sum = 0, count = 0;
    histSnap.forEach((d) => {
      if (d.id === event.params.reportId) return;
      const h = d.data();
      if (h.approvalStatus !== 'approved') return;
      sum += h.grandTotal || 0;
      count++;
    });

    const flags = [];
    if (litres > 0 && revenue === 0) {
      flags.push('Litres sold recorded but revenue is zero — possible data entry error');
    }
    if (count >= 5) {
      const avg = sum / count;
      if (avg > 0 && revenue > avg * multiplier) {
        flags.push(`Revenue KSh ${revenue.toLocaleString()} is ${(revenue / avg).toFixed(1)}x the branch's 14-day average (KSh ${avg.toFixed(0)})`);
      } else if (avg > 0 && revenue > 0 && revenue < avg / multiplier) {
        flags.push(`Revenue KSh ${revenue.toLocaleString()} is unusually low — ${((revenue / avg) * 100).toFixed(0)}% of 14-day average`);
      }
    }

    if (!flags.length) return;

    const message = `\u26a0\ufe0f Delphine Alert \u2014 ${branch}\nReport ${r.date}: ${flags.join('; ')}`;
    await notifyAdmins(db, admin, {
      title: 'Report anomaly',
      message,
      severity: 'warning',
      category: 'report_anomaly',
      meta: { reportId: event.params.reportId, branch, date: r.date, revenue, litres, flags },
    });
    logger.info(`Report anomaly flagged for ${branch} ${r.date}: ${flags.join('; ')}`);
  }
);

/**
 * Anomaly Alerts — Bills
 * -----------------------
 * Fires on every new bill. Flags:
 *   - a duplicate invoice reference for the same vendor
 *   - a bill amount far above that vendor's own historical average
 * Needs at least 3 prior bills from a vendor before flagging size, so a
 * vendor's very first bill is never a false positive.
 */
exports.checkBillAnomaly = onDocumentCreated(
  { document: 'bills/{billId}', secrets: SECRETS },
  async (event) => {
    const snap = event.data;
    if (!snap) return;
    const b = snap.data();
    const vendorId = b.vendorId;
    const amount = b.totalAmount || b.amount || 0;
    if (!vendorId || !amount) return;

    const cfgSnap = await db.collection('settings').doc('alerts').get();
    const cfg = cfgSnap.exists ? cfgSnap.data() : {};
    const multiplier = cfg.billAnomalyMultiplier || 3;

    const flags = [];

    if (b.reference) {
      const dupSnap = await db.collection('bills')
        .where('vendorId', '==', vendorId)
        .where('reference', '==', b.reference)
        .get();
      const others = dupSnap.docs.filter((d) => d.id !== event.params.billId);
      if (others.length > 0) {
        flags.push(`Duplicate invoice reference "${b.reference}" for this vendor (${others.length} other bill(s) use it)`);
      }
    }

    const histSnap = await db.collection('bills').where('vendorId', '==', vendorId).get();
    let sum = 0, count = 0;
    histSnap.forEach((d) => {
      if (d.id === event.params.billId) return;
      const h = d.data();
      sum += h.totalAmount || h.amount || 0;
      count++;
    });
    if (count >= 3) {
      const avg = sum / count;
      if (avg > 0 && amount > avg * multiplier) {
        flags.push(`Bill amount KSh ${amount.toLocaleString()} is ${(amount / avg).toFixed(1)}x this vendor's average (KSh ${avg.toFixed(0)})`);
      }
    }

    if (!flags.length) return;

    const vendorName = b.vendorName || vendorId;
    const message = `\u26a0\ufe0f Delphine Alert \u2014 Bill from ${vendorName}\n${flags.join('; ')}`;
    await notifyAdmins(db, admin, {
      title: 'Bill anomaly',
      message,
      severity: 'warning',
      category: 'bill_anomaly',
      meta: { billId: event.params.billId, vendorId, vendorName, amount, flags },
    });
    logger.info(`Bill anomaly flagged for vendor ${vendorName}: ${flags.join('; ')}`);
  }
);

/**
 * Fuel Loss Reconciliation — Daily
 * ----------------------------------
 * Every evening, compares each branch's physical dip-stick reading for
 * Petrol (PMS) against the book stock in `inventory` (what sales +
 * purchases say should be there). A gap beyond the configured threshold
 * is money — theft, meter drift, evaporation, or a data-entry error —
 * and gets alerted immediately instead of waiting for someone to notice
 * a pattern in a report weeks later.
 *
 * Every day's result (flagged or not) is logged to `fuelLossLog` so you
 * have a full trend, not just the days something went wrong.
 */
exports.dailyFuelReconciliation = onSchedule(
  { schedule: 'every day 20:00', timeZone: 'Africa/Nairobi', secrets: SECRETS },
  async () => {
    const cfgSnap = await db.collection('settings').doc('alerts').get();
    const cfg = cfgSnap.exists ? cfgSnap.data() : {};
    const thresholdPct = cfg.fuelLossThresholdPct != null ? cfg.fuelLossThresholdPct : 2;

    const today = new Date().toISOString().split('T')[0];

    for (const branch of BRANCHES) {
      const dipSnap = await db.collection('dipReadings')
        .where('branch', '==', branch)
        .where('date', '==', today)
        .get();
      if (dipSnap.empty) {
        logger.info(`No dip reading today for ${branch} — skipping reconciliation`);
        continue;
      }

      let latest = null;
      dipSnap.forEach((d) => {
        const v = d.data();
        if (!latest || (v.time || '') > (latest.time || '')) latest = v;
      });
      const physicalLitres = latest.litres || 0;

      const docId = `${branch}_Petrol (PMS)`.replace(/[^a-z0-9]/gi, '_');
      const invSnap = await db.collection('inventory').doc(docId).get();
      const bookLitres = invSnap.exists ? (invSnap.data().qty || 0) : 0;

      const variance = bookLitres - physicalLitres;
      const variancePct = bookLitres !== 0 ? (variance / bookLitres) * 100 : (physicalLitres !== 0 ? 100 : 0);
      const flagged = Math.abs(variancePct) > thresholdPct;

      await db.collection('fuelLossLog').add({
        branch, date: today, physicalLitres, bookLitres, variance, variancePct,
        thresholdPct, flagged,
        createdAt: FieldValue.serverTimestamp(),
      });

      if (flagged) {
        const direction = variance > 0 ? 'short (book higher than physical)' : 'excess (physical higher than book)';
        const message = `\u26a0\ufe0f Delphine Fuel Loss \u2014 ${branch}\n`
          + `Book: ${bookLitres.toFixed(0)}L | Physical: ${physicalLitres.toFixed(0)}L\n`
          + `Variance: ${Math.abs(variance).toFixed(0)}L ${direction} (${Math.abs(variancePct).toFixed(1)}%)`;
        await notifyAdmins(db, admin, {
          title: 'Fuel loss alert',
          message,
          severity: 'critical',
          category: 'fuel_loss',
          meta: { branch, date: today, physicalLitres, bookLitres, variance, variancePct },
        });
        logger.warn(`Fuel loss flagged: ${branch} ${today} variance ${variancePct.toFixed(1)}%`);
      }
    }
  }
);

/**
 * Weekly CEO Digest
 * -------------------
 * Every Monday morning, aggregates the past 7 days into one message:
 * revenue, gross margin (using the same WACC costing the rest of the app
 * uses), expenses, net, per-branch growth vs the prior week, low-stock
 * items, and how many anomalies fired. The full breakdown is saved to
 * `digests` for in-app viewing; the SMS/WhatsApp message is a short
 * summary so it's actually readable on a phone.
 */
const PRODUCT_NAME_MAP = {
  'Petrol (PMS)': 'petrol', 'Petrol': 'petrol', 'PMS': 'petrol',
  '2T Motor Oil': 'p2t', '2T Oil': 'p2t',
  '2T-Kupima': 'p2tk', 'Kupima': 'p2tk',
  'Used Oil': 'puoil',
  'Shell Advance': 'pshell',
};

exports.weeklyCEODigest = onSchedule(
  { schedule: 'every monday 07:00', timeZone: 'Africa/Nairobi', secrets: SECRETS },
  async () => {
    const today = new Date();
    const weekStart = new Date(today); weekStart.setDate(weekStart.getDate() - 7);
    const prevWeekStart = new Date(today); prevWeekStart.setDate(prevWeekStart.getDate() - 14);
    const weekStartStr = weekStart.toISOString().split('T')[0];
    const prevWeekStartStr = prevWeekStart.toISOString().split('T')[0];
    const todayStr = today.toISOString().split('T')[0];

    // ── WACC unit costs, same logic as the client's calculateWACC() ──
    const poSnap = await db.collection('purchaseOrders').where('status', '==', 'received').get();
    const accum = {};
    poSnap.forEach((d) => {
      const po = d.data();
      (po.lineItems || []).forEach((line) => {
        const lineBranch = line.branch === 'All Branches' ? null : line.branch;
        let key = null;
        const desc = (line.description || '').trim();
        if (PRODUCT_NAME_MAP[desc]) key = PRODUCT_NAME_MAP[desc];
        else {
          for (const n of Object.keys(PRODUCT_NAME_MAP)) {
            if (!key && desc.toLowerCase().includes(n.toLowerCase())) key = PRODUCT_NAME_MAP[n];
          }
        }
        if (!key) return;
        const qty = parseFloat(line.qty) || 0, price = parseFloat(line.unitPrice) || 0;
        if (!qty || !price) return;
        const branchesToUse = lineBranch ? [lineBranch] : BRANCHES;
        branchesToUse.forEach((b) => {
          if (!accum[b]) accum[b] = {};
          if (!accum[b][key]) accum[b][key] = { totalCost: 0, totalQty: 0 };
          const sq = lineBranch ? qty : qty / BRANCHES.length;
          accum[b][key].totalCost += sq * price;
          accum[b][key].totalQty += sq;
        });
      });
    });
    const wacc = {};
    Object.keys(accum).forEach((b) => {
      wacc[b] = {};
      Object.keys(accum[b]).forEach((k) => {
        const a = accum[b][k];
        wacc[b][k] = a.totalQty > 0 ? a.totalCost / a.totalQty : 0;
      });
    });

    // ── Reports: this week vs prior week, per branch ──
    const repSnap = await db.collection('reports').where('date', '>=', prevWeekStartStr).get();
    const branchData = {};
    BRANCHES.forEach((b) => { branchData[b] = { thisWeekRev: 0, prevWeekRev: 0, thisWeekCOGS: 0, thisWeekLitres: 0 }; });

    repSnap.forEach((d) => {
      const r = d.data();
      if (r.approvalStatus !== 'approved') return;
      const b = r.branch;
      if (!branchData[b]) return;
      const inThisWeek = r.date >= weekStartStr && r.date <= todayStr;
      const inPrevWeek = r.date >= prevWeekStartStr && r.date < weekStartStr;
      const revenue = r.grandTotal || 0;

      if (inThisWeek) {
        branchData[b].thisWeekRev += revenue;
        const litres = (r.pump && r.pump.metre && r.pump.metre.sold) || 0;
        branchData[b].thisWeekLitres += litres;
        const bWacc = wacc[b] || {};
        branchData[b].thisWeekCOGS += litres * (bWacc.petrol || 0);
        const p = r.products || {};
        ['p2t', 'p2tk', 'puoil', 'pshell'].forEach((k) => {
          const q = (p[k] && p[k].qty) || 0;
          branchData[b].thisWeekCOGS += q * (bWacc[k] || 0);
        });
      } else if (inPrevWeek) {
        branchData[b].prevWeekRev += revenue;
      }
    });

    // ── Expenses this week ──
    const expSnap = await db.collection('expenses').where('date', '>=', weekStartStr).get();
    let totalExpenses = 0;
    expSnap.forEach((d) => {
      const e = d.data();
      if (e.date < weekStartStr || e.date > todayStr) return;
      totalExpenses += e.amount || 0;
    });

    // ── Low stock / deficit inventory ──
    const invSnap = await db.collection('inventory').get();
    const deficits = [];
    invSnap.forEach((d) => {
      const v = d.data();
      if (typeof v.qty === 'number' && v.qty <= 0) {
        deficits.push(`${v.branch || '?'} ${v.product || d.id}: ${v.qty.toFixed(0)}`);
      }
    });

    // ── Alerts fired this week ──
    const alertSnap = await db.collection('alerts')
      .where('createdAt', '>=', admin.firestore.Timestamp.fromDate(weekStart))
      .get();
    const alertCount = alertSnap.size;
    const alertsByCategory = {};
    alertSnap.forEach((d) => {
      const a = d.data();
      alertsByCategory[a.category || 'other'] = (alertsByCategory[a.category || 'other'] || 0) + 1;
    });

    // ── Totals ──
    let totalRev = 0, totalCOGS = 0;
    const branchLines = BRANCHES.map((b) => {
      const d = branchData[b];
      totalRev += d.thisWeekRev;
      totalCOGS += d.thisWeekCOGS;
      const growth = d.prevWeekRev > 0 ? ((d.thisWeekRev - d.prevWeekRev) / d.prevWeekRev) * 100 : null;
      return { branch: b, revenue: d.thisWeekRev, prevRevenue: d.prevWeekRev, growthPct: growth, litres: d.thisWeekLitres };
    });

    const grossProfit = totalRev - totalCOGS;
    const grossMarginPct = totalRev > 0 ? (grossProfit / totalRev) * 100 : 0;
    const netAfterExpenses = grossProfit - totalExpenses;

    const digest = {
      weekStart: weekStartStr, weekEnd: todayStr,
      totalRevenue: totalRev, totalCOGS, grossProfit, grossMarginPct,
      totalExpenses, netAfterExpenses,
      branches: branchLines,
      lowStockCount: deficits.length,
      lowStockItems: deficits.slice(0, 10),
      alertCount, alertsByCategory,
      createdAt: FieldValue.serverTimestamp(),
    };

    const digestRef = await db.collection('digests').add(digest);

    // ── Short SMS/WhatsApp summary ──
    const bestBranch = branchLines.slice().sort((a, b) => b.revenue - a.revenue)[0];
    const lines = [
      `\ud83d\udcca Delphine Weekly Digest (${weekStartStr} to ${todayStr})`,
      `Revenue: KSh ${totalRev.toLocaleString()}`,
      `Gross Margin: ${grossMarginPct.toFixed(1)}%`,
      `Expenses: KSh ${totalExpenses.toLocaleString()}`,
      `Net: KSh ${netAfterExpenses.toLocaleString()}`,
      bestBranch ? `Top branch: ${bestBranch.branch} (KSh ${bestBranch.revenue.toLocaleString()})` : '',
      deficits.length ? `\u26a0\ufe0f ${deficits.length} item(s) out of stock` : '',
      alertCount ? `\u26a0\ufe0f ${alertCount} alert(s) this week` : '\u2713 No anomalies flagged this week',
    ].filter(Boolean);

    await notifyAdmins(db, admin, {
      title: 'Weekly CEO Digest',
      message: lines.join('\n'),
      severity: 'info',
      category: 'weekly_digest',
      meta: { digestId: digestRef.id },
    });
    logger.info(`Weekly digest sent: revenue KSh ${totalRev.toFixed(0)}, ${alertCount} alerts`);
  }
);
