package accountsvr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sethvargo/go-password/password"
	corev1 "k8s.io/api/core/v1"
)

var roleNameRegex = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

type Server struct {
	connString string
	conn       *pgx.Conn
}

const (
	DatabaseKeyDSN      = "dsn"
	DatabaseKeyUsername = "username"
	DatabaseKeyPassword = "password"
	DatabaseKeyHost     = "host"
	DatabaseKeyPort     = "port"
	DatabaseKeySchema   = "schema"
	DatabaseKeyDatabase = "database"
)

func NewServer(ctx context.Context, connString string) (*Server, error) {
	fmt.Printf("Attempting to connect to %s\n", connString)
	s := &Server{
		connString: connString,
	}

	return s, s.Connect(ctx)
	// defer conn.Close(context.Background())
}

func (s *Server) Connect(ctx context.Context) error {
	if s.conn != nil {
		if !s.conn.IsClosed() {
			return nil
		}
	}

	conn, err := pgx.Connect(ctx, s.connString)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return err
	}

	s.conn = conn

	return nil
}

func (s *Server) Close(ctx context.Context) error {
	return s.conn.Close(ctx)
}

func (s *Server) ListUsers(ctx context.Context) []string {
	_ = s.Connect(ctx)

	rows, err := s.conn.Query(ctx, `select * from pg_catalog.pg_user`)
	if err != nil {
		return []string{}
	}
	defer rows.Close()

	o := []string{}

	for rows.Next() {
		if err != nil {
			return o
		}
		o = append(o, fmt.Sprintf("%#v", rows.RawValues()))
	}

	return o
}

// TODO actually generate password
func (s *Server) generatePassword() string {
	res, err := password.Generate(28, 10, 1, false, false)
	if err != nil {
		log.Fatal(err)
	}
	return res
}

func (s *Server) IsRole(ctx context.Context, roleName string) (bool, error) {
	_ = s.Connect(ctx)

	rows, err := s.conn.Query(ctx, `select usename from pg_catalog.pg_user where usename=$1`, roleName)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func (s *Server) IsDatabase(ctx context.Context, dbName string) (string, bool, error) {
	_ = s.Connect(ctx)

	dbName = roleNameRegex.ReplaceAllString(dbName, "")
	rows, err := s.conn.Query(ctx, `SELECT FROM pg_database WHERE datname = $1`, dbName)
	if err != nil {
		return dbName, false, err
	}
	defer rows.Close()

	return dbName, rows.Next(), nil
}

var ErrRoleExists = errors.New("role already exists")

func (s *Server) CreateRole(ctx context.Context, roleName string) (string, string, error) {
	_ = s.Connect(ctx)

	if v, err := s.IsRole(ctx, roleName); err != nil || v {
		if v {
			return "", "", ErrRoleExists
		}

		return "", "", err
	}

	password := s.generatePassword()
	roleName = roleNameRegex.ReplaceAllString(roleName, "")
	stmt := fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, roleName, password)
	_, err := s.conn.Exec(ctx, stmt)
	if err != nil {
		return "", "", err
	}

	return roleName, password, nil
}

func (s *Server) UpdateRolePassword(ctx context.Context, roleName string) (string, string, error) {
	_ = s.Connect(ctx)

	password := s.generatePassword()
	roleName = roleNameRegex.ReplaceAllString(roleName, "")
	stmt := fmt.Sprintf(`ALTER ROLE %s LOGIN PASSWORD '%s'`, roleName, password)
	_, err := s.conn.Exec(ctx, stmt)
	if err != nil {
		return "", "", err
	}

	return roleName, password, nil
}

func (s *Server) CreateDatabase(ctx context.Context, dbName, roleName string) (string, error) {
	_ = s.Connect(ctx)

	dbName = roleNameRegex.ReplaceAllString(dbName, "")
	roleName = roleNameRegex.ReplaceAllString(roleName, "")
	stmt := fmt.Sprintf(`CREATE DATABASE %s OWNER %s`, dbName, roleName)

	_, err := s.conn.Exec(ctx, stmt)
	if err != nil {
		return "", err
	}

	return dbName, nil
}

func (s *Server) CreateSchema(ctx context.Context, schemaName, roleName string) error {
	_ = s.Connect(ctx)

	schemaName = roleNameRegex.ReplaceAllString(schemaName, "")
	roleName = roleNameRegex.ReplaceAllString(roleName, "")
	stmt := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION %s`, schemaName, roleName)

	_, err := s.conn.Exec(ctx, stmt)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) CopyConfigToSecret(secret *corev1.Secret) {
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	secret.Data[DatabaseKeyHost] = []byte(s.conn.Config().Host)
	secret.Data[DatabaseKeyPort] = []byte(fmt.Sprintf("%d", s.conn.Config().Port))
}

func getSecretKV(secret *corev1.Secret, key string) string {
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	if secret.StringData == nil {
		secret.StringData = make(map[string]string)
	}

	if v, ok := secret.Data[key]; ok {
		return string(v)
	}

	if v, ok := secret.Data[key]; ok {
		return fmt.Sprintf("%s", v)
	}

	return ""
}

func GenerateDSN(secret *corev1.Secret) string {
	var host string

	if len(getSecretKV(secret, DatabaseKeyPort)) > 0 {
		host = fmt.Sprintf("%s:%s", getSecretKV(secret, DatabaseKeyHost), getSecretKV(secret, DatabaseKeyPort))
	} else {
		host = getSecretKV(secret, DatabaseKeyHost)
	}

	u := &url.URL{
		User: url.UserPassword(
			getSecretKV(secret, DatabaseKeyUsername),
			getSecretKV(secret, DatabaseKeyPassword),
		),
		Host:   host,
		Scheme: "postgres",
		Path:   getSecretKV(secret, DatabaseKeyDatabase),
	}

	return u.String()
}

func (s *Server) Delete(ctx context.Context, name string) error {
	_ = s.Connect(ctx)

	name = roleNameRegex.ReplaceAllString(name, "")

	_, err := s.conn.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s WITH (FORCE)`, name))
	if err != nil {
		if !strings.Contains(err.Error(), " not found") {
			return err
		}
	}

	_, err = s.conn.Exec(ctx, fmt.Sprintf(`DROP ROLE IF EXISTS %s`, name))
	if err != nil {
		if !strings.Contains(err.Error(), " not found") {
			return err
		}
	}

	return nil
}
