package db

import (
	"context"
	"fmt"
)

// devinSourceUUIDScopeVersion is the data version at which the Devin
// parser began emitting session-scoped message source identities.
// Earlier archives store bare node_id/step_id values, which are only
// unique within a session and collide across sessions in usage
// deduplication.
const devinSourceUUIDScopeVersion = 110

// scopeLegacyDevinSourceUUIDsLocked rewrites stored Devin message source
// identities from the bare pre-110 form to the session-scoped form the
// parser now emits, so an archive written by an older binary holds the
// same values a fresh parse produces. It runs once, before the full
// resync that the version bump triggers, on archives still below
// devinSourceUUIDScopeVersion. Rewriting in place means every consumer
// of source_uuid (pin restore, orphan copies, recall evidence, usage
// facts, mirror pushes) sees one identity form and needs no translation.
//
// Session ids are "devin:<raw>" locally and "host~devin:<raw>" for remote
// hosts; host names cannot contain '~' or ':', so the raw id is the text
// after the "devin:" prefix. A scoped value is "<raw>:<id>". Values that
// already contain ':' are left alone, so the rewrite is idempotent when
// startup repeats before the resync completes.
//
// Every rewritten session gets a transcript revision bump: source_uuid
// participates in transcript revision equality, and mirrors use the
// revision to notice changed rows.
func scopeLegacyDevinSourceUUIDsLocked(
	ctx context.Context, w *writerHandle,
) error {
	var version int
	if err := w.QueryRowContext(
		ctx, "PRAGMA user_version",
	).Scan(&version); err != nil {
		return fmt.Errorf("probing data version: %w", err)
	}
	if version >= devinSourceUUIDScopeVersion {
		return nil
	}

	tx, err := w.Begin()
	if err != nil {
		return fmt.Errorf("beginning devin source uuid rewrite: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	const devinSessions = `SELECT id FROM sessions
		WHERE id LIKE 'devin:%' OR id LIKE '%~devin:%'`
	const scoped = `substr(session_id, instr(session_id, 'devin:') + 6)
		|| ':' || `

	rows, err := tx.Query(`SELECT DISTINCT session_id FROM messages
		WHERE session_id IN (` + devinSessions + `)
		  AND (
			(source_uuid != '' AND instr(source_uuid, ':') = 0)
			OR (source_parent_uuid != ''
				AND instr(source_parent_uuid, ':') = 0)
		  )`)
	if err != nil {
		return fmt.Errorf("finding legacy devin source uuids: %w", err)
	}
	var sessionIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scanning devin session id: %w", err)
		}
		sessionIDs = append(sessionIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("listing devin sessions: %w", err)
	}
	rows.Close()
	if len(sessionIDs) == 0 {
		return nil
	}

	if _, err := tx.Exec(`UPDATE messages
		SET source_uuid = ` + scoped + `source_uuid
		WHERE session_id IN (` + devinSessions + `)
		  AND source_uuid != '' AND instr(source_uuid, ':') = 0`,
	); err != nil {
		return fmt.Errorf("scoping devin source uuids: %w", err)
	}
	if _, err := tx.Exec(`UPDATE messages
		SET source_parent_uuid = ` + scoped + `source_parent_uuid
		WHERE session_id IN (` + devinSessions + `)
		  AND source_parent_uuid != ''
		  AND instr(source_parent_uuid, ':') = 0`,
	); err != nil {
		return fmt.Errorf("scoping devin source parent uuids: %w", err)
	}
	for _, id := range sessionIDs {
		if err := bumpTranscriptRevision(tx, id, true); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing devin source uuid rewrite: %w", err)
	}
	return nil
}
