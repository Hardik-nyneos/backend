// Update function for joined exposures (header, line item, bucketing)
const updateExposureHeadersLineItemsBucketing = async (req, res) => {
  const { exposure_header_id } = req.params;
  const {
    headerFields = {},
    lineItemFields = {},
    bucketingFields = {},
    hedgingFields = {},
  } = req.body;
  if (!exposure_header_id) {
    return res.status(400).json({ error: "Invalid exposure_header_id" });
  }
  try {
    let updated = {};

    // Ensure exposure_bucketing row exists
    if (Object.keys(bucketingFields).length > 0) {
      const { rows: bucketingRows } = await pool.query(
        "SELECT 1 FROM exposure_bucketing WHERE exposure_header_id = $1",
        [exposure_header_id]
      );
      if (bucketingRows.length === 0) {
        await pool.query(
          "INSERT INTO exposure_bucketing (exposure_header_id) VALUES ($1)",
          [exposure_header_id]
        );
      }
    }

    // Ensure hedging_proposal row exists
    if (Object.keys(hedgingFields || {}).length > 0) {
      const { rows: hedgingRows } = await pool.query(
        "SELECT 1 FROM hedging_proposal WHERE exposure_header_id = $1",
        [exposure_header_id]
      );
      if (hedgingRows.length === 0) {
        await pool.query(
          "INSERT INTO hedging_proposal (exposure_header_id) VALUES ($1)",
          [exposure_header_id]
        );
      }
    }

    // Update exposure_headers
    if (Object.keys(headerFields).length > 0) {
      const setClause = Object.keys(headerFields)
        .map((key, i) => `"${key}" = $${i + 1}`)
        .join(", ");
      const values = Object.values(headerFields);
      const query = `UPDATE exposure_headers SET ${setClause} WHERE exposure_header_id = $${
        values.length + 1
      } RETURNING *`;
      const result = await pool.query(query, [...values, exposure_header_id]);
      if (result.rowCount > 0) updated.header = result.rows[0];
      // If header changes, set bucketing status to 'pending'
      await pool.query(
        `UPDATE exposure_bucketing SET status = 'pending' WHERE exposure_header_id = $1`,
        [exposure_header_id]
      );
    }
    // Update exposure_line_items (all line items for this header)
    if (Object.keys(lineItemFields).length > 0) {
      const setClause = Object.keys(lineItemFields)
        .map((key, i) => `"${key}" = $${i + 1}`)
        .join(", ");
      const values = Object.values(lineItemFields);
      const query = `UPDATE exposure_line_items SET ${setClause} WHERE exposure_header_id = $${
        values.length + 1
      } RETURNING *`;
      const result = await pool.query(query, [...values, exposure_header_id]);
      if (result.rowCount > 0) updated.lineItems = result.rows;
      // If line item changes, set bucketing status to 'pending'
      await pool.query(
        `UPDATE exposure_bucketing SET status = 'pending' WHERE exposure_header_id = $1`,
        [exposure_header_id]
      );
    }

    // Update exposure_bucketing (all bucketing rows for this header)
    if (Object.keys(bucketingFields).length > 0) {
      // Update fields
      const setClause = Object.keys(bucketingFields)
        .map((key, i) => `"${key}" = $${i + 1}`)
        .join(", ");
      const values = Object.values(bucketingFields);
      const query = `UPDATE exposure_bucketing SET ${setClause} WHERE exposure_header_id = $${
        values.length + 1
      } RETURNING *`;
      const result = await pool.query(query, [...values, exposure_header_id]);
      // Set status to 'Pending' if any change
      await pool.query(
        `UPDATE exposure_bucketing SET status = 'pending' WHERE exposure_header_id = $1`,
        [exposure_header_id]
      );
      if (result.rowCount > 0) updated.bucketing = result.rows;
    }

    // Update hedging_proposal (all rows for this header)
    if (Object.keys(hedgingFields || {}).length > 0) {
      // Update fields
      const setClause = Object.keys(hedgingFields)
        .map((key, i) => `"${key}" = $${i + 1}`)
        .join(", ");
      const values = Object.values(hedgingFields);
      const query = `UPDATE hedging_proposal SET ${setClause} WHERE exposure_header_id = $${
        values.length + 1
      } RETURNING *`;
      const result = await pool.query(query, [...values, exposure_header_id]);
      // Set status to 'Pending' if any change
      await pool.query(
        `UPDATE hedging_proposal SET status = 'pending' WHERE exposure_header_id = $1`,
        [exposure_header_id]
      );
      if (result.rowCount > 0) updated.hedging = result.rows;
    }

    if (Object.keys(updated).length === 0) {
      return res.status(404).json({ error: "No records updated" });
    }
    res.json({ success: true, updated });
  } catch (err) {
    console.error("Error updating joined exposures:", err);
    res.status(500).json({ error: "Failed to update joined exposures" });
  }
};

const globalSession = require("../globalSession");
const { pool } = require("../db");
const csv = require("csv-parser");
const path = require("path");
const fs = require("fs");

const getUserVars = async (req, res) => {
  const session = globalSession.UserSessions[0];

  if (!session) {
    return res.status(404).json({ error: "No active session found" });
  }

  const [firstName, ...restName] = session.name?.split(" ") || ["", ""];
  const secondName = restName.join(" ") || "";

  const loginDate = new Date(session.lastLoginTime || new Date());
  const dateLoggedIn = loginDate.toISOString().split("T")[0]; // "YYYY-MM-DD"
  const timeLoggedIn = loginDate.toTimeString().split(" ")[0]; // "HH:MM:SS"

  try {
    const query = "SELECT * FROM notifications WHERE user_id = $1";
    const result = await pool.query(query, [session.userId]);

    const messages = result.rows.map((row) => ({
      date: row.date,
      priority: row.priority,
      deadline: row.deadline,
      text: row.text,
    }));

    const userVars = {
      roleName: session.role,
      firstName,
      secondName,
      dateLoggedIn,
      timeLoggedIn,
      isLoggedIn: session.isLoggedIn,
      userEmailId: session.email,
      notification: {
        messages,
      },
    };

    res.json(userVars);
  } catch (err) {
    console.error("Error fetching notifications:", err);
    res.status(500).json({ error: "Failed to fetch notifications" });
  }
};

const approveBucketing = async (req, res) => {
  const { exposureIds, approved_by, approval_comment } = req.body;
  console.log(exposureIds);

  if (!Array.isArray(exposureIds) || exposureIds.length === 0 || !approved_by) {
    return res.status(400).json({
      success: false,
      message: "exposureIds and approved_by are required",
    });
  }

  try {
    // Fetch current statuses
    const { rows: existingExposures } = await pool.query(
      `SELECT id, status_bucketing FROM exposures WHERE id = ANY($1::uuid[])`,
      [exposureIds]
    );

    const toDelete = existingExposures
      .filter((row) => row.status_bucketing === "Delete-Approval")
      .map((row) => row.id);

    const toApprove = existingExposures
      .filter((row) => row.status_bucketing !== "Delete-Approval")
      .map((row) => row.id);

    const results = {
      deleted: [],
      approved: [],
    };

    // Delete exposures
    if (toDelete.length > 0) {
      const deleted = await pool.query(
        `DELETE FROM exposures WHERE id = ANY($1::uuid[]) RETURNING *`,
        [toDelete]
      );
      results.deleted = deleted.rows;
    }

    // Approve remaining exposures
    if (toApprove.length > 0) {
      const approved = await pool.query(
        `UPDATE exposures
         SET status_bucketing = 'Approved'
         WHERE id = ANY($1::uuid[])
         RETURNING *`,
        [toApprove]
      );
      results.approved = approved.rows;
    }

    res.status(200).json({ success: true, ...results });
  } catch (err) {
    console.error("approveMultipleExposures error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
};

const rejectMultipleExposures = async (req, res) => {
  const { exposureIds, rejected_by, rejection_comment } = req.body;

  if (!Array.isArray(exposureIds) || exposureIds.length === 0 || !rejected_by) {
    return res.status(400).json({
      success: false,
      message: "exposureIds and rejected_by are required",
    });
  }

  try {
    const { rows: existingExposures } = await pool.query(
      `SELECT id, status_bucketing FROM exposures WHERE id = ANY($1::uuid[])`,
      [exposureIds]
    );

    const toDelete = existingExposures
      .filter((row) => row.status_bucketing === "Delete-Approval")
      .map((row) => row.id);

    const toReject = existingExposures
      .filter((row) => row.status_bucketing !== "Delete-Approval")
      .map((row) => row.id);

    const results = {
      deleted: [],
      rejected: [],
    };

    // Delete exposures
    if (toDelete.length > 0) {
      const deleted = await pool.query(
        `DELETE FROM exposures WHERE id = ANY($1::uuid[]) RETURNING *`,
        [toDelete]
      );
      results.deleted = deleted.rows;
    }

    // Reject remaining exposures
    if (toReject.length > 0) {
      const rejected = await pool.query(
        `UPDATE exposures
         SET status_bucketing = 'Rejected'
         WHERE id = ANY($1::uuid[])
         RETURNING *`,
        [toReject]
      );
      results.rejected = rejected.rows;
    }

    res.status(200).json({ success: true, ...results });
  } catch (err) {
    console.error("rejectMultipleExposures error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
};

const getRenderVars = async (req, res) => {
  try {
    // 1. Get current user session
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
    const userId = session.userId;

    // 2. Get user's business unit name
    const userResult = await pool.query(
      "SELECT business_unit_name FROM users WHERE id = $1",
      [userId]
    );
    if (!userResult.rows.length) {
      return res.status(404).json({ error: "User not found" });
    }
    const userBu = userResult.rows[0].business_unit_name;
    if (!userBu) {
      return res
        .status(404)
        .json({ error: "User has no business unit assigned" });
    }

    // 3. Find all descendant business units using recursive CTE
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "Business unit entity not found" });
    }
    const rootEntityId = entityResult.rows[0].entity_id;
    const descendantsResult = await pool.query(
      `
      WITH RECURSIVE descendants AS (
        SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
        UNION ALL
        SELECT me.entity_id, me.entity_name
        FROM masterEntity me
        INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
        INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
        WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
      )
      SELECT entity_name FROM descendants
    `,
      [rootEntityId]
    );
    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }

    // 4. Filter exposures by business_unit in buNames and status approved
    const exposuresResult = await pool.query(
      `SELECT * FROM exposures WHERE (status = 'approved' OR status = 'Approved') AND business_unit = ANY($1)`,
      [buNames]
    );
    const exposureIds = exposuresResult.rows.map((row) => row.id);

    if (exposureIds.length > 0) {
      await pool.query(
        `UPDATE exposures SET status_bucketing = 'Pending' WHERE id = ANY($1::uuid[])`,
        [exposureIds]
      );
    }

    const updatedRows = exposuresResult.rows.map((row) => ({
      ...row,
      status_bucketing: "Pending",
    }));

    // Fetch permissions for 'exposure-bucketing' page for this role
    const roleName = session.role;
    let exposureBucketingPerms = {};
    if (roleName) {
      const roleResult = await pool.query(
        "SELECT id FROM roles WHERE name = $1",
        [roleName]
      );
      if (roleResult.rows.length > 0) {
        const role_id = roleResult.rows[0].id;
        const permResult = await pool.query(
          `SELECT p.page_name, p.tab_name, p.action, rp.allowed
           FROM role_permissions rp
           JOIN permissions p ON rp.permission_id = p.id
           WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')`,
          [role_id]
        );
        // Build permissions structure for 'exposure-bucketing'
        for (const row of permResult.rows) {
          if (row.page_name !== "exposure-bucketing") continue;
          const tab = row.tab_name;
          const action = row.action;
          const allowed = row.allowed;
          if (!exposureBucketingPerms["exposure-bucketing"])
            exposureBucketingPerms["exposure-bucketing"] = {};
          if (tab === null) {
            if (!exposureBucketingPerms["exposure-bucketing"].pagePermissions)
              exposureBucketingPerms["exposure-bucketing"].pagePermissions = {};
            exposureBucketingPerms["exposure-bucketing"].pagePermissions[
              action
            ] = allowed;
          } else {
            if (!exposureBucketingPerms["exposure-bucketing"].tabs)
              exposureBucketingPerms["exposure-bucketing"].tabs = {};
            if (!exposureBucketingPerms["exposure-bucketing"].tabs[tab])
              exposureBucketingPerms["exposure-bucketing"].tabs[tab] = {};
            exposureBucketingPerms["exposure-bucketing"].tabs[tab][action] =
              allowed;
          }
        }
      }
    }
    res.json({
      ...(exposureBucketingPerms["exposure-bucketing"]
        ? { "exposure-bucketing": exposureBucketingPerms["exposure-bucketing"] }
        : {}),
      buAccessible: buNames,
      pageData: updatedRows,
    });
  } catch (err) {
    console.error("Error fetching or updating exposures:", err);
    res.status(500).json({ error: "Failed to fetch exposures" });
  }
};

const getUserJourney = (req, res) => {
  res.json({
    process: "viewAllExposures",
    nextPageToCall: "exposure-Bucketing",
    actionCalledFrom: "submit",
  });
};

// In your Express route file
const getupdate = async (req, res) => {
  const { id } = req.params;
  const updatedFields = req.body;
  console.log("Updating exposure with ID:", id, "Fields:", updatedFields);

  if (!id || Object.keys(updatedFields).length === 0) {
    return res.status(400).json({ error: "Invalid ID or no fields to update" });
  }

  try {
    const setClause = Object.keys(updatedFields)
      .map((key, i) => `"${key}" = $${i + 1}`)
      .join(", ");

    const values = Object.values(updatedFields);

    const query = `UPDATE exposures SET ${setClause} WHERE id = $${
      values.length + 1
    } RETURNING *`;
    const result = await pool.query(query, [...values, id]);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Exposure not found" });
    }

    res.json({ success: true, updated: result.rows[0] });
  } catch (err) {
    console.error("Error updating exposure:", err);
    res.status(500).json({ error: "Failed to update exposure" });
  }
};

const getExposureHeadersLineItemsBucketing = async (req, res) => {
  try {
    // 1. Get current user session
    const session = globalSession.UserSessions[0];
    if (!session) {
      return res.status(404).json({ error: "No active session found" });
    }
    const userId = session.userId;

    // 2. Get user's business unit name
    const userResult = await pool.query(
      "SELECT business_unit_name FROM users WHERE id = $1",
      [userId]
    );
    if (!userResult.rows.length) {
      return res.status(404).json({ error: "User not found" });
    }
    const userBu = userResult.rows[0].business_unit_name;
    if (!userBu) {
      return res
        .status(404)
        .json({ error: "User has no business unit assigned" });
    }

    // 3. Find all descendant business units using recursive CTE
    const entityResult = await pool.query(
      "SELECT entity_id FROM masterEntity WHERE entity_name = $1 AND (approval_status = 'Approved' OR approval_status = 'approved') AND (is_deleted = false OR is_deleted IS NULL)",
      [userBu]
    );
    if (!entityResult.rows.length) {
      return res.status(404).json({ error: "Business unit entity not found" });
    }
    const rootEntityId = entityResult.rows[0].entity_id;
    const descendantsResult = await pool.query(
      `
      WITH RECURSIVE descendants AS (
        SELECT entity_id, entity_name FROM masterEntity WHERE entity_id = $1
        UNION ALL
        SELECT me.entity_id, me.entity_name
        FROM masterEntity me
        INNER JOIN entityRelationships er ON me.entity_id = er.child_entity_id
        INNER JOIN descendants d ON er.parent_entity_id = d.entity_id
        WHERE (me.approval_status = 'Approved' OR me.approval_status = 'approved') AND (me.is_deleted = false OR me.is_deleted IS NULL)
      )
      SELECT entity_name FROM descendants
      `,
      [rootEntityId]
    );
    const buNames = descendantsResult.rows.map((r) => r.entity_name);
    if (!buNames.length) {
      return res
        .status(404)
        .json({ error: "No accessible business units found" });
    }

    // 4. Ensure all exposure_header_id are present in exposure_bucketing
    await pool.query(
      `INSERT INTO exposure_bucketing (exposure_header_id)
       SELECT exposure_header_id
       FROM exposure_headers
       WHERE entity = ANY($1)
         AND (approval_status = 'approved' OR approval_status = 'Approved')
         AND exposure_header_id NOT IN (
           SELECT exposure_header_id FROM exposure_bucketing
         )`,
      [buNames]
    );

    // 5. Join exposure_headers, exposure_line_items, and exposure_bucketing
    const joinResult = await pool.query(
      `SELECT h.*, l.*, b.*
       FROM exposure_headers h
       JOIN exposure_line_items l ON h.exposure_header_id = l.exposure_header_id
       LEFT JOIN exposure_bucketing b ON h.exposure_header_id = b.exposure_header_id
       WHERE h.entity = ANY($1)
         AND (h.approval_status = 'approved' OR h.approval_status = 'Approved')`,
      [buNames]
    );
    const updatedRows = joinResult.rows;

    // Fetch permissions for 'exposure-bucketing' page for this role
    const roleName = session.role;
    let exposureBucketingPerms = {};
    if (roleName) {
      const roleResult = await pool.query(
        "SELECT id FROM roles WHERE name = $1",
        [roleName]
      );
      if (roleResult.rows.length > 0) {
        const role_id = roleResult.rows[0].id;
        const permResult = await pool.query(
          `SELECT p.page_name, p.tab_name, p.action, rp.allowed
           FROM role_permissions rp
           JOIN permissions p ON rp.permission_id = p.id
           WHERE rp.role_id = $1 AND (rp.status = 'Approved' OR rp.status = 'approved')`,
          [role_id]
        );
        // Build permissions structure for 'exposure-bucketing'
        for (const row of permResult.rows) {
          if (row.page_name !== "exposure-bucketing") continue;
          const tab = row.tab_name;
          const action = row.action;
          const allowed = row.allowed;
          if (!exposureBucketingPerms["exposure-bucketing"])
            exposureBucketingPerms["exposure-bucketing"] = {};
          if (tab === null) {
            if (!exposureBucketingPerms["exposure-bucketing"].pagePermissions)
              exposureBucketingPerms["exposure-bucketing"].pagePermissions = {};
            exposureBucketingPerms["exposure-bucketing"].pagePermissions[
              action
            ] = allowed;
          } else {
            if (!exposureBucketingPerms["exposure-bucketing"].tabs)
              exposureBucketingPerms["exposure-bucketing"].tabs = {};
            if (!exposureBucketingPerms["exposure-bucketing"].tabs[tab])
              exposureBucketingPerms["exposure-bucketing"].tabs[tab] = {};
            exposureBucketingPerms["exposure-bucketing"].tabs[tab][action] =
              allowed;
          }
        }
      }
    }
    res.json({
      ...(exposureBucketingPerms["exposure-bucketing"]
        ? { "exposure-bucketing": exposureBucketingPerms["exposure-bucketing"] }
        : {}),
      buAccessible: buNames,
      pageData: updatedRows,
    });
  } catch (err) {
    console.error("Error fetching joined exposures:", err);
    res.status(500).json({ error: "Failed to fetch joined exposures" });
  }
};
// Approve exposure_bucketing rows (status only, no delete logic, updated columns)
const approveBucketingStatus = async (req, res) => {
  const { exposure_header_ids, updated_by, comments } = req.body;
  if (
    !Array.isArray(exposure_header_ids) ||
    exposure_header_ids.length === 0 ||
    !updated_by
  ) {
    return res
      .status(400)
      .json({
        success: false,
        message: "exposure_header_ids and updated_by are required",
      });
  }
  try {
    const { rows: updated } = await pool.query(
      `UPDATE exposure_bucketing
       SET status_bucketing = 'Approved', updated_by = $2, comments = $3, updated_at = NOW()
       WHERE exposure_header_id = ANY($1)
       RETURNING *`,
      [exposure_header_ids, updated_by, comments || null]
    );
    res.status(200).json({ success: true, Approved: updated });
  } catch (err) {
    console.error("approveBucketingStatus error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
};

// Reject exposure_bucketing rows (status only, no delete logic, updated columns)
const rejectBucketingStatus = async (req, res) => {
  const { exposure_header_ids, updated_by, comments } = req.body;
  if (
    !Array.isArray(exposure_header_ids) ||
    exposure_header_ids.length === 0 ||
    !updated_by
  ) {
    return res
      .status(400)
      .json({
        success: false,
        message: "exposure_header_ids and updated_by are required",
      });
  }
  try {
    const { rows: updated } = await pool.query(
      `UPDATE exposure_bucketing
       SET status_bucketing = 'Rejected', updated_by = $2, comments = $3, updated_at = NOW()
       WHERE exposure_header_id = ANY($1)
       RETURNING *`,
      [exposure_header_ids, updated_by, comments || null]
    );
    res.status(200).json({ success: true, Rejected: updated });
  } catch (err) {
    console.error("rejectBucketingStatus error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
};

module.exports = {
  getUserVars,
  getRenderVars,
  getUserJourney,
  getupdate,
  approveBucketing,
  rejectMultipleExposures,
  getExposureHeadersLineItemsBucketing,
  updateExposureHeadersLineItemsBucketing,
  approveBucketingStatus,
  rejectBucketingStatus,
};
