package redis

import (
	"context"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/zukadong/crond/driver"
	"log"
	"time"
)

const ctxTimeout = 2 * time.Second

type driverRedis struct {
	driver     *redis.Client
	ctx        context.Context
	expiration time.Duration
	cancelCh   chan struct{}
}

func NewDriver(r *redis.Client) driver.Driver {
	return &driverRedis{ctx: context.Background(), driver: r, cancelCh: make(chan struct{})}
}

func (r *driverRedis) Ping() error { return r.driver.Ping(r.ctx).Err() }

func (r *driverRedis) SetKeepaliveInterval(interval time.Duration) { r.expiration = interval }

func (r *driverRedis) Keepalive(nodeId string) { go r.keepalive(nodeId) }

func (r *driverRedis) GetServiceNodeList(serviceName string) ([]string, error) {
	var list []string

	// crond:{serviceName}:*
	match := driver.PrefixKey + driver.JAR + serviceName + driver.JAR + driver.REG

	iter := r.driver.Scan(r.ctx, 0, match, 0).Iterator()
	for iter.Next(r.ctx) {
		list = append(list, iter.Val())

		err := iter.Err()
		if err != nil {
			return list, err
		}
	}

	return list, nil
}

func (r *driverRedis) UnRegisterServiceNode() { r.cancelCh <- struct{}{} }

func (r *driverRedis) RegisterServiceNode(serviceName string) (string, error) {
	//              crond:{serviceName}:{uuid}
	nodeId := driver.PrefixKey + driver.JAR + serviceName + driver.JAR + uuid.NewString()

	return nodeId, r.register(nodeId)
}

func (r *driverRedis) register(nodeId string) error {
	ctx, cancel := context.WithTimeout(r.ctx, ctxTimeout)

	err := r.driver.SetEx(ctx, nodeId, "ok", r.expiration).Err()

	cancel()

	return err
}

func (r *driverRedis) unregister(nodeId string) error {
	ctx, cancel := context.WithTimeout(r.ctx, ctxTimeout)

	err := r.driver.Del(ctx, nodeId).Err()
	cancel()

	return err
}

func (r *driverRedis) keepalive(nodeId string) {
	ticker := time.NewTicker(r.expiration / 2)
	defer ticker.Stop()

	for {
		select {

		case <-r.cancelCh:
			err := r.unregister(nodeId)
			if err != nil {
				log.Printf("error: node[%s] unregister failed: [%+v]", nodeId, err)
			}

			return

		case <-ticker.C:
			ctx, cancel := context.WithTimeout(r.ctx, ctxTimeout)
			ok, err := r.driver.Expire(ctx, nodeId, r.expiration).Result()
			cancel()

			if err != nil {
				log.Printf("error: node[%s] renewal failed: [%+v]", nodeId, err)
			}

			if !ok {
				if err := r.register(nodeId); err != nil {
					log.Printf("error: node[%s] register failed: [%+v]", nodeId, err)
				}
			}
		}
	}
}
