package claim

import (
	"encoding/base64"
	"testing"

	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

func TestNormalizeUploadedFileSupportsUTF16LE(t *testing.T) {
	rawJSON := `{"account_id":"acct-utf16","access_token":"token-utf16","refresh_token":"refresh-utf16","note":"中文"}`
	encoded, _, err := transform.String(unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewEncoder(), rawJSON)
	if err != nil {
		t.Fatalf("encode utf-16 payload: %v", err)
	}

	normalized, contentJSON, err := normalizeUploadedFile(uploadFileInput{
		Name:          "utf16.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(encoded)),
	}, 64*1024)
	if err != nil {
		t.Fatalf("normalize utf-16 upload: %v", err)
	}

	if normalizedUploadString(normalized, "account_id") != "acct-utf16" {
		t.Fatalf("unexpected account_id: %#v", normalized["account_id"])
	}
	if normalizedUploadString(normalized, "note") != "中文" {
		t.Fatalf("unexpected note field: %#v", normalized["note"])
	}
	if contentJSON == "" {
		t.Fatal("expected normalized content json to be populated")
	}
}

func TestNormalizeUploadedFileSupportsGBK(t *testing.T) {
	rawJSON := `{"account_id":"acct-gbk","access_token":"token-gbk","refresh_token":"refresh-gbk","note":"中文"}`
	encoded, _, err := transform.String(simplifiedchinese.GBK.NewEncoder(), rawJSON)
	if err != nil {
		t.Fatalf("encode gbk payload: %v", err)
	}

	normalized, _, err := normalizeUploadedFile(uploadFileInput{
		Name:          "gbk.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(encoded)),
	}, 64*1024)
	if err != nil {
		t.Fatalf("normalize gbk upload: %v", err)
	}

	if normalizedUploadString(normalized, "account_id") != "acct-gbk" {
		t.Fatalf("unexpected account_id: %#v", normalized["account_id"])
	}
	if normalizedUploadString(normalized, "note") != "中文" {
		t.Fatalf("unexpected note field: %#v", normalized["note"])
	}
}
