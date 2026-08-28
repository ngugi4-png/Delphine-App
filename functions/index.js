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
const { logger } = require('firebase-functions');
const admin = require('firebase-admin');

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
