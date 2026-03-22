package claim

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"token-atlas/internal/runtimecache"
)

type tokenDocument struct {
	Index    int
	ID       string
	Name     string
	Path     string
	Size     int
	MTime    string
	Encoding string
	Keys     []string
	Error    string
	Content  any
}

func (d tokenDocument) toIndexPayload() map[string]any {
	return map[string]any{
		"index":    d.Index,
		"id":       d.ID,
		"name":     d.Name,
		"path":     d.Path,
		"size":     d.Size,
		"mtime":    d.MTime,
		"encoding": d.Encoding,
		"keys":     d.Keys,
		"error":    nullableStringOrNil(d.Error),
	}
}

func (d tokenDocument) toDetailPayload() map[string]any {
	return map[string]any{
		"item":    d.toIndexPayload(),
		"content": d.Content,
	}
}

type claimFileItem struct {
	ClaimID  int64
	FileName string
	FilePath string
	Encoding string
	Content  any
}

type claimedTokenItem struct {
	TokenID  int64
	FileName string
	FilePath string
	Encoding string
	Content  any
}

func (s *Service) ListClaimFiles(ctx context.Context, userID int64) ([]claimFileItem, error) {
	return runtimecache.CacheJSON(s.cache, s.claimFilesCacheKey(userID), s.cfg.Cache.ClaimsTTL, func() ([]claimFileItem, error) {
		rows, err := s.store.DB().QueryContext(ctx, `
			SELECT token_claims.id AS claim_id,
			       COALESCE(token_claims.claim_file_name, tokens.file_name) AS file_name,
			       COALESCE(token_claims.claim_file_path, tokens.file_path) AS file_path,
			       COALESCE(token_claims.claim_encoding, tokens.encoding) AS encoding,
			       COALESCE(token_claims.claim_content_json, tokens.content_json) AS content_json
			FROM token_claims
			LEFT JOIN tokens ON tokens.id = token_claims.token_id
			WHERE token_claims.user_id = ? AND token_claims.is_hidden = 0
			ORDER BY token_claims.claimed_at_ts DESC, token_claims.id DESC
		`, userID)
		if err != nil {
			return nil, fmt.Errorf("list claim files: %w", err)
		}
		defer rows.Close()

		items := make([]claimFileItem, 0)
		for rows.Next() {
			var (
				item        claimFileItem
				contentJSON string
			)
			if err := rows.Scan(&item.ClaimID, &item.FileName, &item.FilePath, &item.Encoding, &contentJSON); err != nil {
				return nil, fmt.Errorf("scan claim file row: %w", err)
			}
			content, err := decodeJSONContent(contentJSON)
			if err != nil {
				return nil, wrapCorruptClaimDataError(err)
			}
			item.Content = content
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate claim file rows: %w", err)
		}
		return items, nil
	})
}

func (s *Service) ListClaimedTokens(ctx context.Context, userID int64) ([]claimedTokenItem, error) {
	return runtimecache.CacheJSON(s.cache, s.claimedDocumentsCacheKey(userID), s.cfg.Cache.ClaimsTTL, func() ([]claimedTokenItem, error) {
		rows, err := s.store.DB().QueryContext(ctx, `
			SELECT token_claims.token_id AS token_id,
			       COALESCE(token_claims.claim_file_name, tokens.file_name) AS file_name,
			       COALESCE(token_claims.claim_file_path, tokens.file_path) AS file_path,
			       COALESCE(token_claims.claim_encoding, tokens.encoding) AS encoding,
			       COALESCE(token_claims.claim_content_json, tokens.content_json) AS content_json,
			       MAX(token_claims.claimed_at_ts) AS last_claimed_ts
			FROM token_claims
			LEFT JOIN tokens ON tokens.id = token_claims.token_id
			WHERE token_claims.user_id = ? AND token_claims.is_hidden = 0
			GROUP BY token_claims.token_id, file_name, file_path, encoding, content_json
			ORDER BY last_claimed_ts DESC, token_claims.token_id DESC
		`, userID)
		if err != nil {
			return nil, fmt.Errorf("list claimed tokens: %w", err)
		}
		defer rows.Close()

		items := make([]claimedTokenItem, 0)
		for rows.Next() {
			var (
				item          claimedTokenItem
				contentJSON   string
				lastClaimedTS sql.NullInt64
			)
			if err := rows.Scan(&item.TokenID, &item.FileName, &item.FilePath, &item.Encoding, &contentJSON, &lastClaimedTS); err != nil {
				return nil, fmt.Errorf("scan claimed token row: %w", err)
			}
			content, err := decodeJSONContent(contentJSON)
			if err != nil {
				return nil, err
			}
			item.Content = content
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate claimed token rows: %w", err)
		}
		return items, nil
	})
}

func (s *Service) buildClaimedDocuments(ctx context.Context, userID int64) ([]tokenDocument, error) {
	items, err := s.ListClaimedTokens(ctx, userID)
	if err != nil {
		return nil, err
	}

	documents := make([]tokenDocument, 0, len(items))
	for index, item := range items {
		content := item.Content
		keys := make([]string, 0)
		if mapping, ok := content.(map[string]any); ok {
			for key := range mapping {
				keys = append(keys, key)
			}
			sortStrings(keys)
		}

		size := len(mustJSON(content))
		mtime := isoformatNow()
		fullPath := filepath.Join(".", filepath.FromSlash(item.FilePath))
		if stat, err := os.Stat(fullPath); err == nil {
			size = int(stat.Size())
			mtime = stat.ModTime().In(time.Local).Format(time.RFC3339)
		}

		documents = append(documents, tokenDocument{
			Index:    index,
			ID:       strings.TrimSuffix(item.FileName, filepath.Ext(item.FileName)),
			Name:     item.FileName,
			Path:     item.FilePath,
			Size:     size,
			MTime:    mtime,
			Encoding: item.Encoding,
			Keys:     keys,
			Content:  content,
		})
	}
	return documents, nil
}

func buildZipBuffer(items []zipItem) (*bytes.Buffer, int, error) {
	buffer := &bytes.Buffer{}
	archive := zip.NewWriter(buffer)
	for _, item := range items {
		writer, err := archive.Create(item.Name)
		if err != nil {
			_ = archive.Close()
			return nil, 0, err
		}
		encoded, err := json.Marshal(item.Content)
		if err != nil {
			_ = archive.Close()
			return nil, 0, err
		}
		if _, err := writer.Write(encoded); err != nil {
			_ = archive.Close()
			return nil, 0, err
		}
	}
	if err := archive.Close(); err != nil {
		return nil, 0, err
	}
	return buffer, buffer.Len(), nil
}

type zipItem struct {
	Name    string
	Content any
}

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return encoded
}

func sortStrings(values []string) {
	if len(values) < 2 {
		return
	}
	for index := 1; index < len(values); index++ {
		for cursor := index; cursor > 0 && values[cursor] < values[cursor-1]; cursor-- {
			values[cursor], values[cursor-1] = values[cursor-1], values[cursor]
		}
	}
}
