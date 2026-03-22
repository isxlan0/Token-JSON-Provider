package claim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

type textDecoderCandidate struct {
	name   string
	decode func([]byte) (string, error)
}

func detectAndDecodeText(raw []byte) (string, string, error) {
	candidates := decoderCandidates(raw)

	for _, candidate := range candidates {
		decoded, err := candidate.decode(raw)
		if err != nil {
			continue
		}
		trimmed := trimLeadingBOM(decoded)
		if json.Valid([]byte(trimmed)) {
			return candidate.name, trimmed, nil
		}
	}

	for _, candidate := range candidates {
		decoded, err := candidate.decode(raw)
		if err != nil {
			continue
		}
		return candidate.name, trimLeadingBOM(decoded), nil
	}

	return "", "", fmt.Errorf("unable to determine encoding")
}

func decoderCandidates(raw []byte) []textDecoderCandidate {
	candidates := make([]textDecoderCandidate, 0, 8)
	added := make(map[string]struct{})
	add := func(candidate textDecoderCandidate) {
		if _, ok := added[candidate.name]; ok {
			return
		}
		added[candidate.name] = struct{}{}
		candidates = append(candidates, candidate)
	}

	if bytes.HasPrefix(raw, []byte{0xEF, 0xBB, 0xBF}) {
		add(textDecoderCandidate{name: "utf-8-sig", decode: decodeUTF8})
	}
	if bytes.HasPrefix(raw, []byte{0xFF, 0xFE}) {
		add(textDecoderCandidate{name: "utf-16-le", decode: decodeUTF16LE})
	}
	if bytes.HasPrefix(raw, []byte{0xFE, 0xFF}) {
		add(textDecoderCandidate{name: "utf-16-be", decode: decodeUTF16BE})
	}

	add(textDecoderCandidate{name: "utf-8", decode: decodeUTF8})
	add(textDecoderCandidate{name: "utf-16", decode: decodeUTF16BOM})
	add(textDecoderCandidate{name: "utf-16-le", decode: decodeUTF16LE})
	add(textDecoderCandidate{name: "utf-16-be", decode: decodeUTF16BE})
	add(textDecoderCandidate{name: "gbk", decode: decodeGBK})
	add(textDecoderCandidate{name: "big5", decode: decodeBig5})
	add(textDecoderCandidate{name: "latin-1", decode: decodeLatin1})

	return candidates
}

func decodeUTF8(raw []byte) (string, error) {
	trimmed := raw
	if bytes.HasPrefix(trimmed, []byte{0xEF, 0xBB, 0xBF}) {
		trimmed = trimmed[3:]
	}
	if !utf8.Valid(trimmed) {
		return "", fmt.Errorf("invalid utf-8")
	}
	return string(trimmed), nil
}

func decodeUTF16BOM(raw []byte) (string, error) {
	decoded, _, err := transform.String(unicode.UTF16(unicode.LittleEndian, unicode.ExpectBOM).NewDecoder(), string(raw))
	if err != nil {
		return "", err
	}
	return decoded, nil
}

func decodeUTF16LE(raw []byte) (string, error) {
	decoded, _, err := transform.String(unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder(), string(raw))
	if err != nil {
		return "", err
	}
	return decoded, nil
}

func decodeUTF16BE(raw []byte) (string, error) {
	decoded, _, err := transform.String(unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder(), string(raw))
	if err != nil {
		return "", err
	}
	return decoded, nil
}

func decodeGBK(raw []byte) (string, error) {
	decoded, _, err := transform.String(simplifiedchinese.GBK.NewDecoder(), string(raw))
	if err != nil {
		return "", err
	}
	return decoded, nil
}

func decodeBig5(raw []byte) (string, error) {
	decoded, _, err := transform.String(traditionalchinese.Big5.NewDecoder(), string(raw))
	if err != nil {
		return "", err
	}
	return decoded, nil
}

func decodeLatin1(raw []byte) (string, error) {
	decoded, _, err := transform.String(charmap.ISO8859_1.NewDecoder(), string(raw))
	if err != nil {
		return "", err
	}
	return decoded, nil
}

func trimLeadingBOM(value string) string {
	return strings.TrimPrefix(value, "\ufeff")
}
