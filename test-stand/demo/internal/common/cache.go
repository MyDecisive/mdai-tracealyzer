package common

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Redis struct {
	client  *redis.Client
	service string
}

func NewRedis(ctx context.Context, service, addr string) (*Redis, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Redis{client: client, service: service}, nil
}

func (r *Redis) Close() error {
	return r.client.Close()
}

func (r *Redis) Get(ctx context.Context, key string) (string, bool, error) {
	span, ctx := ddtracer.StartSpanFromContext(ctx, "redis.get",
		ddtracer.ServiceName(r.service),
		ddtracer.ResourceName("GET "+key),
		ddtracer.Tag(ext.SpanKind, ext.SpanKindClient),
	)
	span.SetTag("db.system", "redis")
	span.SetTag("db.statement", "GET "+key)
	span.SetTag("db.operation", "GET")
	defer span.Finish()

	val, err := r.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		span.SetTag(ext.Error, err)
		return "", false, err
	}
	return val, true, nil
}

func (r *Redis) Set(ctx context.Context, key, value string) error {
	span, ctx := ddtracer.StartSpanFromContext(ctx, "redis.set",
		ddtracer.ServiceName(r.service),
		ddtracer.ResourceName("SET "+key),
		ddtracer.Tag(ext.SpanKind, ext.SpanKindClient),
	)
	span.SetTag("db.system", "redis")
	span.SetTag("db.statement", "SET "+key)
	span.SetTag("db.operation", "SET")
	defer span.Finish()

	if err := r.client.Set(ctx, key, value, 0).Err(); err != nil {
		span.SetTag(ext.Error, err)
		return err
	}
	return nil
}
