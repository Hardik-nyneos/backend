const { Client } = require("pg");
const multer = require("multer");
const csv = require("csv-parser");
const fs = require("fs");

const upload = multer({ dest: "uploads/" });

const db = new Client({
  user: "avnadmin",
  password: "AVNS_L6PcvF7OBRIZu5QDpZ4",
  host: "pg-nyneos-kanavlt885-nyneos.g.aivencloud.com",
  port: 15247,
  database: "defaultdb",
  ssl: { rejectUnauthorized: false },
});

db.connect();

exports.uploadCsv = [
  upload.single("file"),
  async (req, res) => {
    if (!req.file) return res.status(400).json({ error: "No file uploaded" });
    const results = [];
    const filePath = req.file.path;
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", async () => {
        try {
          for (const row of results) {
            const ref_no = row.ref_no?.trim();
            const type = row.type?.trim().toLowerCase();
            const bu = row.bu?.trim();
            const vendor_beneficiary = row.vendor_beneficiary?.trim() || null;
            const amount = parseFloat(row.amount) || 0;
            const quantity = parseFloat(row.quantity) || 0;
            const maturity_expiry = row.maturity_expiry
              ? new Date(row.maturity_expiry)
              : null;
            const details = row.details?.trim() || null;
            const currency = row.currency?.trim() || null;
            const status = row.status?.trim() || null;
            const uploaded_by = row.uploaded_by?.trim() || "system";
            if (!ref_no || !type || !bu) continue;
            await db.query(
              `INSERT INTO page1_records
                (ref_no, type, bu, vendor_beneficiary, amount, quantity, maturity_expiry, details, currency, status, uploaded_by)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               ON CONFLICT (ref_no) DO NOTHING`,
              [
                ref_no,
                type,
                bu,
                vendor_beneficiary,
                amount,
                quantity,
                maturity_expiry,
                details,
                currency,
                status,
                uploaded_by,
              ]
            );
          }
          fs.unlinkSync(filePath);
          res.status(200).json({ message: "CSV uploaded and saved to DB ✅" });
        } catch (error) {
          res
            .status(500)
            .json({
              error: "Failed to insert CSV rows ❌",
              details: error.message,
            });
        }
      });
  },
];

exports.getExposureBucketing = async (req, res) => {
  try {
    const query = `
      SELECT 
        p1.ref_no AS "poNumber",
        p1.vendor_beneficiary AS client,
        p1.type,
        p1.bu,
        p1.details,
        p1.maturity_expiry AS date,
        p1.currency,
        p1.amount,
        COALESCE(p2.advance_given, 0) AS advance,
        COALESCE(p2.inco, '') AS inco,
        COALESCE(p2.month_1, 0) AS m1,
        COALESCE(p2.month_2, 0) AS m2,
        COALESCE(p2.month_3, 0) AS m3,
        COALESCE(p2.month_4_6, 0) AS m4to6,
        COALESCE(p2.month_gt_6, 0) AS m6p,
        COALESCE(p2.remarks, '') AS remarks
      FROM page1_records p1
      LEFT JOIN page2_records p2 ON p1.ref_no = p2.ref_no
      WHERE p1.type IN ('payable', 'receivable')
    `;
    const result = await db.query(query);
    res.json({
      payload: {
        showSaveBtn: true,
        showResetBtn: true,
        showPrintBtn: true,
        showExportBtn: true,
        data: result.rows,
      },
      pageToCall: "exposureBucketing",
    });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Failed to fetch exposure data", details: error.message });
  }
};

exports.saveExposureBucketing = async (req, res) => {
  try {
    const { data } = req.body;
    for (const record of data) {
      await db.query(
        `
        INSERT INTO page2_records (
          ref_no, vendor, type, bu, details, maturity_date, 
          currency, amount, advance_given, inco, 
          month_1, month_2, month_3, month_4_6, month_gt_6, remarks
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        ON CONFLICT (ref_no) DO UPDATE SET
          vendor = EXCLUDED.vendor,
          type = EXCLUDED.type,
          bu = EXCLUDED.bu,
          details = EXCLUDED.details,
          maturity_date = EXCLUDED.maturity_date,
          currency = EXCLUDED.currency,
          amount = EXCLUDED.amount,
          advance_given = EXCLUDED.advance_given,
          inco = EXCLUDED.inco,
          month_1 = EXCLUDED.month_1,
          month_2 = EXCLUDED.month_2,
          month_3 = EXCLUDED.month_3,
          month_4_6 = EXCLUDED.month_4_6,
          month_gt_6 = EXCLUDED.month_gt_6,
          remarks = EXCLUDED.remarks,
          updated_at = CURRENT_TIMESTAMP
      `,
        [
          record.poNumber,
          record.client,
          record.type,
          record.bu,
          record.details,
          record.date,
          record.currency,
          record.amount,
          record.advance,
          record.inco,
          record.m1,
          record.m2,
          record.m3,
          record.m4to6,
          record.m6p,
          record.remarks,
        ]
      );
    }
    res.json({ success: true, message: "Data saved successfully" });
  } catch (error) {
    res.status(500).json({ error: "Failed to save exposure data" });
  }
};

exports.getHedgingProposal = async (req, res) => {
  try {
    const result = await db.query(`
      SELECT 
        bu,
        currency,
        COALESCE(month_1, 0) AS month_1,
        COALESCE(month_2, 0) AS month_2,
        COALESCE(month_3, 0) AS month_3,
        COALESCE(month_4_6, 0) AS month_4_6,
        COALESCE(month_gt_6, 0) AS month_gt_6
      FROM page2_records
      ORDER BY bu, currency
    `);
    res.json({
      payload: {
        showSaveBtn: true,
        showResetBtn: true,
        showPrintBtn: true,
        hedgingData: result.rows,
      },
    });
  } catch (error) {
    res
      .status(500)
      .json({ error: "Failed to fetch hedging data", details: error.message });
  }
};

exports.saveHedgingProposal = async (req, res) => {
  try {
    const data = req.body;
    await db.query("BEGIN");
    for (const record of data) {
      await db.query(
        `
        INSERT INTO page2_records (bu, currency, month_1, month_2, month_3, month_4_6, month_gt_6)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (bu, currency) DO UPDATE SET
          month_1 = EXCLUDED.month_1,
          month_2 = EXCLUDED.month_2,
          month_3 = EXCLUDED.month_3,
          month_4_6 = EXCLUDED.month_4_6,
          month_gt_6 = EXCLUDED.month_gt_6
      `,
        [
          record.bu,
          record.currency,
          record.month_1,
          record.month_2,
          record.month_3,
          record.month_4_6,
          record.month_gt_6,
        ]
      );
    }
    await db.query("COMMIT");
    res.json({ success: true });
  } catch (error) {
    await db.query("ROLLBACK");
    res.status(500).json({ error: "Failed to save hedging data" });
  }
};
