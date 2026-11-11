package postgres

// A minimal PostgreSQL frontend (v3.0 protocol) over TCP. It lets the
// helper reach its server without a database driver: it supports trust,
// cleartext, md5 and SCRAM-SHA-256 authentication and simple queries, which
// is all creating, tagging and dropping a database needs.

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// row is one result row in text format.
type row []*string

type pgConn struct {
	conn net.Conn
	r    *bufio.Reader
}

// handshake performs the startup exchange. user and password are used when
// the server asks for them; database is the target database.
func handshake(conn net.Conn, user, password, database string) (c *pgConn, err error) {
	c = &pgConn{conn: conn, r: bufio.NewReaderSize(conn, 16<<10)}
	var body bytes.Buffer
	_ = binary.Write(&body, binary.BigEndian, int32(196608)) // protocol 3.0
	for _, kv := range [][2]string{
		{"user", user},
		{"database", database},
		{"application_name", "plinth/testkit"},
		{"client_encoding", "UTF8"},
	} {
		body.WriteString(kv[0])
		body.WriteByte(0)
		body.WriteString(kv[1])
		body.WriteByte(0)
	}
	body.WriteByte(0)
	if err := c.writeUntyped(body.Bytes()); err != nil {
		return nil, err
	}
	var scram *scramState
	for {
		t, payload, err := c.read()
		if err != nil {
			return nil, err
		}
		switch t {
		case 'R':
			if len(payload) < 4 {
				return nil, errors.New("postgres: short authentication message")
			}
			switch code := binary.BigEndian.Uint32(payload[:4]); code {
			case 0: // AuthenticationOk
			case 3: // cleartext
				if err := c.write('p', append([]byte(password), 0)); err != nil {
					return nil, err
				}
			case 5: // md5
				if len(payload) < 8 {
					return nil, errors.New("postgres: short md5 salt")
				}
				if err := c.write('p', append([]byte(md5Password(user, password, payload[4:8])), 0)); err != nil {
					return nil, err
				}
			case 10: // SASL
				var mechs []string
				for _, m := range strings.Split(string(payload[4:]), "\x00") {
					if m != "" {
						mechs = append(mechs, m)
					}
				}
				if !containsString(mechs, "SCRAM-SHA-256") {
					return nil, fmt.Errorf("postgres: no supported SASL mechanism in %v", mechs)
				}
				scram, err = newScram(password)
				if err != nil {
					return nil, err
				}
				first := scram.clientFirstResponse()
				var b bytes.Buffer
				b.WriteString("SCRAM-SHA-256\x00")
				_ = binary.Write(&b, binary.BigEndian, int32(len(first)))
				b.WriteString(first)
				if err := c.write('p', b.Bytes()); err != nil {
					return nil, err
				}
			case 11: // SASL continue
				if scram == nil {
					return nil, errors.New("postgres: SASL continue without start")
				}
				resp, err := scram.clientFinal(string(payload[4:]))
				if err != nil {
					return nil, err
				}
				if err := c.write('p', []byte(resp)); err != nil {
					return nil, err
				}
			case 12: // SASL final
				if scram == nil {
					return nil, errors.New("postgres: SASL final without start")
				}
				if err := scram.verifyServerFinal(string(payload[4:])); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("postgres: unsupported authentication method %d", code)
			}
		case 'S', 'K', 'N':
			// ParameterStatus, BackendKeyData, NoticeResponse: not needed.
		case 'E':
			return nil, readError(payload)
		case 'Z':
			return c, nil
		default:
			return nil, fmt.Errorf("postgres: unexpected message %q during startup", t)
		}
	}
}

func (c *pgConn) write(t byte, payload []byte) error {
	b := make([]byte, 0, len(payload)+5)
	b = append(b, t)
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(payload)+4))
	b = append(b, n[:]...)
	b = append(b, payload...)
	_, err := c.conn.Write(b)
	return err
}

func (c *pgConn) writeUntyped(body []byte) error {
	b := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(b, uint32(len(body)+4))
	b = append(b, body...)
	_, err := c.conn.Write(b)
	return err
}

func (c *pgConn) read() (byte, []byte, error) {
	t, err := c.r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var n [4]byte
	if _, err := io.ReadFull(c.r, n[:]); err != nil {
		return 0, nil, err
	}
	size := int(binary.BigEndian.Uint32(n[:])) - 4
	if size < 0 || size > 64<<20 {
		return 0, nil, fmt.Errorf("postgres: invalid message length %d", size+4)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(c.r, payload); err != nil {
		return 0, nil, err
	}
	return t, payload, nil
}

// close ends the session.
func (c *pgConn) close() { _ = c.conn.Close() }

// exec runs one simple query and returns its rows in text format.
func (c *pgConn) exec(query string) ([]row, error) {
	if err := c.write('Q', append([]byte(query), 0)); err != nil {
		return nil, err
	}
	var rows []row
	var firstErr error
	for {
		t, payload, err := c.read()
		if err != nil {
			return nil, err
		}
		switch t {
		case 'D':
			r, err := readDataRow(payload)
			if err != nil {
				return nil, err
			}
			rows = append(rows, r)
		case 'E':
			if firstErr == nil {
				firstErr = readError(payload)
			}
		case 'Z':
			return rows, firstErr
		}
	}
}

func readDataRow(payload []byte) (row, error) {
	if len(payload) < 2 {
		return nil, errors.New("postgres: short data row")
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	payload = payload[2:]
	r := make(row, 0, n)
	for i := 0; i < n; i++ {
		if len(payload) < 4 {
			return nil, errors.New("postgres: short data row field")
		}
		l := int32(binary.BigEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if l < 0 {
			r = append(r, nil)
			continue
		}
		if int(l) > len(payload) {
			return nil, errors.New("postgres: short data row value")
		}
		s := string(payload[:l])
		r = append(r, &s)
		payload = payload[l:]
	}
	return r, nil
}

// readError decodes an ErrorResponse into a plain message.
func readError(payload []byte) error {
	fields := map[byte]string{}
	for len(payload) > 0 && payload[0] != 0 {
		code := payload[0]
		payload = payload[1:]
		i := bytes.IndexByte(payload, 0)
		if i < 0 {
			break
		}
		fields[code] = string(payload[:i])
		payload = payload[i+1:]
	}
	msg := strings.TrimSpace(fields['M'])
	if sqlState := fields['C']; sqlState != "" {
		msg = fmt.Sprintf("%s (SQLSTATE %s)", msg, sqlState)
	}
	if msg == "" {
		msg = "unknown server error"
	}
	return errors.New("postgres: " + msg)
}

// md5Password computes the response to an md5 authentication request.
func md5Password(user, password string, salt []byte) string {
	inner := md5.Sum([]byte(password + user))
	outer := md5.Sum(append([]byte(hex.EncodeToString(inner[:])), salt...))
	return "md5" + hex.EncodeToString(outer[:])
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// scramState is the client side of SCRAM-SHA-256 (RFC 5802, RFC 7677) with
// channel binding not supported.
type scramState struct {
	password     string
	clientNonce  string
	clientFirst  string
	serverSigB64 string
}

func newScram(password string) (*scramState, error) {
	var nb [18]byte
	if _, err := rand.Read(nb[:]); err != nil {
		return nil, err
	}
	nonce := base64.RawStdEncoding.EncodeToString(nb[:])
	// n= is empty: the server takes the user name from the startup message.
	return &scramState{password: password, clientNonce: nonce, clientFirst: "n=,r=" + nonce}, nil
}

// clientFirstResponse returns the SASL initial response: the gs2 header
// followed by the client-first message.
func (s *scramState) clientFirstResponse() string { return "n,," + s.clientFirst }

// clientFinal returns the client-final message for the server-first message.
func (s *scramState) clientFinal(serverFirst string) (string, error) {
	attrs := parseSCRAM(serverFirst)
	if attrs["m"] != "" {
		return "", errors.New("postgres: unsupported SCRAM extension")
	}
	nonce, saltB64, iterStr := attrs["r"], attrs["s"], attrs["i"]
	if !strings.HasPrefix(nonce, s.clientNonce) || len(nonce) <= len(s.clientNonce) {
		return "", errors.New("postgres: server SCRAM nonce does not extend client nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return "", errors.New("postgres: bad SCRAM salt")
	}
	iter, err := strconv.Atoi(iterStr)
	if err != nil || iter < 1 {
		return "", errors.New("postgres: bad SCRAM iteration count")
	}
	salted := pbkdf2SHA256([]byte(s.password), salt, iter, sha256.Size)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + nonce
	authMsg := s.clientFirst + "," + serverFirst + "," + withoutProof
	clientSig := hmacSHA256(storedKey[:], []byte(authMsg))
	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	serverKey := hmacSHA256(salted, []byte("Server Key"))
	s.serverSigB64 = base64.StdEncoding.EncodeToString(hmacSHA256(serverKey, []byte(authMsg)))
	return withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof), nil
}

func (s *scramState) verifyServerFinal(serverFinal string) error {
	attrs := parseSCRAM(serverFinal)
	if e := attrs["e"]; e != "" {
		return errors.New("postgres: SCRAM server error: " + e)
	}
	if s.serverSigB64 == "" || attrs["v"] != s.serverSigB64 {
		return errors.New("postgres: SCRAM server signature mismatch")
	}
	return nil
}

func parseSCRAM(s string) map[string]string {
	m := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		if len(part) >= 2 && part[1] == '=' {
			m[part[:1]] = part[2:]
		}
	}
	return m
}

func hmacSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

// pbkdf2SHA256 is PBKDF2-HMAC-SHA-256 (RFC 8018) producing dkLen bytes.
func pbkdf2SHA256(password, salt []byte, iter, dkLen int) []byte {
	prf := hmac.New(sha256.New, password)
	hashLen := prf.Size()
	blocks := (dkLen + hashLen - 1) / hashLen
	dk := make([]byte, 0, blocks*hashLen)
	u := make([]byte, hashLen)
	var idx [4]byte
	for block := 1; block <= blocks; block++ {
		prf.Reset()
		prf.Write(salt)
		binary.BigEndian.PutUint32(idx[:], uint32(block))
		prf.Write(idx[:])
		dk = prf.Sum(dk) // U_1
		copy(u, dk[len(dk)-hashLen:])
		blockOut := dk[len(dk)-hashLen:]
		for n := 2; n <= iter; n++ {
			prf.Reset()
			prf.Write(u)
			u = prf.Sum(u[:0])
			for i := range u {
				blockOut[i] ^= u[i]
			}
		}
	}
	return dk[:dkLen]
}
