package database

import "database/sql"

type User struct {
	ID              int64
	LinuxDOUserID   string
	LinuxDOUsername string
	LinuxDOName     sql.NullString
	TrustLevel      int64
	CreatedAtTS     int64
	LastLoginAtTS   int64
}

type APIKey struct {
	ID           int64
	UserID       int64
	Name         sql.NullString
	KeyHash      string
	KeyPrefix    string
	KeyValue     sql.NullString
	Status       string
	CreatedAtTS  int64
	LastUsedAtTS sql.NullInt64
	RevokedAtTS  sql.NullInt64
}

type Token struct {
	ID               int64
	FileName         string
	FilePath         string
	FileHash         string
	Encoding         string
	ContentJSON      string
	AccountID        sql.NullString
	AccessTokenHash  sql.NullString
	ProviderUserID   sql.NullString
	ProviderUsername sql.NullString
	ProviderName     sql.NullString
	UploadedAtTS     sql.NullInt64
	IsActive         int64
	IsCleaned        int64
	IsEnabled        int64
	IsBanned         int64
	IsAvailable      int64
	ClaimCount       int64
	MaxClaims        int64
	CreatedAtTS      int64
	BannedAtTS       sql.NullInt64
	BanReason        sql.NullString
	CleanedAtTS      sql.NullInt64
	LastProbeAtTS    sql.NullInt64
	LastProbeStatus  sql.NullString
	ProbeLockUntilTS sql.NullInt64
	UpdatedAtTS      int64
	LastSeenAtTS     int64
}

type TokenClaim struct {
	ID               int64
	TokenID          int64
	UserID           int64
	APIKeyID         sql.NullInt64
	ClaimedAtTS      int64
	IsHidden         int64
	ClaimFileName    sql.NullString
	ClaimFilePath    sql.NullString
	ClaimEncoding    sql.NullString
	ClaimContentJSON sql.NullString
	ProviderUserID   sql.NullString
	ProviderUsername sql.NullString
	ProviderName     sql.NullString
	RequestID        string
}

type UserTokenClaim struct {
	ID           int64
	UserID       int64
	TokenID      int64
	FirstClaimID int64
	CreatedAtTS  int64
}

type ClaimQueueItem struct {
	ID           int64
	UserID       int64
	APIKeyID     sql.NullInt64
	Requested    int64
	Remaining    int64
	QueueRank    int64
	EnqueuedAtTS int64
	RequestID    string
	Status       string
}

type UserBan struct {
	ID               int64
	LinuxDOUserID    string
	UsernameSnapshot sql.NullString
	Reason           string
	BannedByUserID   int64
	BannedAtTS       int64
	ExpiresAtTS      sql.NullInt64
	UnbannedByUserID sql.NullInt64
	UnbannedAtTS     sql.NullInt64
}

type InventoryRuntime struct {
	ID          int64
	Status      string
	MaxClaims   int64
	UpdatedAtTS int64
}

type QueueRuntime struct {
	ID          int64
	TotalQueued int64
	UpdatedAtTS int64
}
