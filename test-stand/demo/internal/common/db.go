package common

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Postgres struct {
	conn    *pgx.Conn
	dbName  string
	service string
}

func NewPostgres(ctx context.Context, service, dsn string) (*Postgres, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Postgres{conn: conn, dbName: cfg.Database, service: service}, nil
}

func (p *Postgres) Close(ctx context.Context) error {
	return p.conn.Close(ctx)
}

func (p *Postgres) Query(ctx context.Context, operation, statement string, args ...any) (pgx.Rows, error) {
	span, ctx := ddtracer.StartSpanFromContext(ctx, "postgres.query",
		ddtracer.ServiceName(p.service),
		ddtracer.ResourceName(operation),
		ddtracer.Tag(ext.SpanKind, ext.SpanKindClient),
	)
	span.SetTag("db.system", "postgresql")
	span.SetTag("db.name", p.dbName)
	span.SetTag("db.statement", statement)
	span.SetTag("db.operation", operation)

	rows, err := p.conn.Query(ctx, statement, args...)
	if err != nil {
		span.SetTag(ext.Error, err)
	}
	span.Finish()
	return rows, err
}
