package claim

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"token-atlas/internal/runtimecache"
)

type claimedDownloadItem struct {
	FileName string
	Content  any
}

type claimDownloadAccessSummary struct {
	UserHiddenClaim   bool
	OtherVisibleClaim bool
}

func (s *Service) GetClaimedTokenForDownload(ctx context.Context, tokenID int64, userID int64) (*claimedDownloadItem, error) {
	return runtimecache.CacheJSON(s.cache, s.claimedTokenCacheKey(userID, tokenID), s.cfg.Cache.ClaimsTTL, func() (*claimedDownloadItem, error) {
		row := s.store.DB().QueryRowContext(ctx, `
			SELECT COALESCE(token_claims.claim_file_name, tokens.file_name) AS file_name,
			       COALESCE(token_claims.claim_content_json, tokens.content_json) AS content_json
			FROM token_claims
			LEFT JOIN tokens ON tokens.id = token_claims.token_id
			WHERE token_claims.token_id = ?
			  AND token_claims.user_id = ?
			  AND token_claims.is_hidden = 0
			ORDER BY token_claims.claimed_at_ts DESC, token_claims.id DESC
			LIMIT 1
		`, tokenID, userID)

		var (
			fileName    string
			contentJSON string
		)
		if err := row.Scan(&fileName, &contentJSON); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, fmt.Errorf("query claimed token for download: %w", err)
		}

		content, err := decodeJSONContent(contentJSON)
		if err != nil {
			return nil, wrapCorruptClaimDataError(err)
		}

		return &claimedDownloadItem{
			FileName: fileName,
			Content:  content,
		}, nil
	})
}

func (s *Service) GetClaimDownloadAccessSummary(ctx context.Context, tokenID int64, userID int64) (claimDownloadAccessSummary, error) {
	var summary claimDownloadAccessSummary

	userHidden, err := queryCount(ctx, s.store.DB(), `
		SELECT COUNT(*)
		FROM token_claims
		WHERE token_id = ? AND user_id = ? AND is_hidden = 1
	`, tokenID, userID)
	if err != nil {
		return summary, err
	}

	otherVisible, err := queryCount(ctx, s.store.DB(), `
		SELECT COUNT(*)
		FROM token_claims
		WHERE token_id = ? AND user_id != ? AND is_hidden = 0
	`, tokenID, userID)
	if err != nil {
		return summary, err
	}

	summary.UserHiddenClaim = userHidden > 0
	summary.OtherVisibleClaim = otherVisible > 0
	return summary, nil
}

func encodeDownloadContent(item *claimedDownloadItem) ([]byte, error) {
	if item == nil {
		return nil, nil
	}
	contentJSON, err := json.Marshal(item.Content)
	if err != nil {
		return nil, fmt.Errorf("encode claimed download content: %w", err)
	}
	return contentJSON, nil
}

func buildContentDisposition(fileName string) string {
	safeName := strings.TrimSpace(fileName)
	if safeName == "" {
		safeName = "claimed-token.json"
	}
	return fmt.Sprintf(`attachment; filename="%s"; filename*=UTF-8''%s`, escapeContentDispositionFilename(safeName), url.PathEscape(safeName))
}

func escapeContentDispositionFilename(fileName string) string {
	replacer := strings.NewReplacer("\\", "_", "\"", "_", "\r", "_", "\n", "_")
	return replacer.Replace(fileName)
}
