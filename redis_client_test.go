package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var ctx = context.Background()

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})

func TestClient(t *testing.T) {
	assert.NotNil(t, client)

	// err := client.Close()
	// assert.Nil(t, err)
}

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "2x", "ko", time.Second*3)

	result, _ := client.Get(ctx, "2x").Result()
	assert.Equal(t, "ko", result)

	time.Sleep(time.Second * 5)
	result, err := client.Get(ctx, "2x").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	client.RPush(ctx, "shoto", "Ryu")
	client.RPush(ctx, "shoto", "Ken")
	client.RPush(ctx, "shoto", "Sakura")

	assert.Equal(t, "Ryu", client.LPop(ctx, "shoto").Val())
	assert.Equal(t, "Sakura", client.RPop(ctx, "shoto").Val())
	assert.Equal(t, "Ken", client.LPop(ctx, "shoto").Val())

	client.Del(ctx, "shoto")
}

func TestSets(t *testing.T) {
	client.SAdd(ctx, "2xko", "Ahri")
	client.SAdd(ctx, "2xko", "Ahri")
	client.SAdd(ctx, "2xko", "Yasuo")
	client.SAdd(ctx, "2xko", "Yasuo")
	client.SAdd(ctx, "2xko", "Darius")
	client.SAdd(ctx, "2xko", "Darius")

	assert.Equal(t, int64(3), client.SCard(ctx, "2xko").Val())
	assert.Equal(t, []string{"Darius", "Yasuo", "Ahri"}, client.SMembers(ctx, "2xko").Val())
}

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "prices", redis.Z{Score: 25000, Member: "Cheese Burger"})
	client.ZAdd(ctx, "prices", redis.Z{Score: 45000, Member: "Bento"})
	client.ZAdd(ctx, "prices", redis.Z{Score: 65000, Member: "Stuffed Crust Pizza"})

	assert.Equal(
		t,
		[]string{"Cheese Burger", "Bento", "Stuffed Crust Pizza"},
		client.ZRange(ctx, "prices", 0, -1).Val(),
	)
	assert.Equal(t, "Stuffed Crust Pizza", client.ZPopMax(ctx, "prices").Val()[0].Member)
	assert.Equal(t, "Bento", client.ZPopMax(ctx, "prices").Val()[0].Member)
	assert.Equal(t, "Cheese Burger", client.ZPopMax(ctx, "prices").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user:1", "id", "1")
	client.HSet(ctx, "user:1", "name", "Yukari Takeba")
	client.HSet(ctx, "user:1", "address", "Tatsumi Port Island Dorm")

	user := client.HGetAll(ctx, "user:1").Val()
	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "Yukari Takeba", user["name"])
	assert.Equal(t, "Tatsumi Port Island Dorm", user["address"])

	client.Del(ctx, "user:1")
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Indomaret Fresh Jatiwarna",
		Longitude: 106.92421739209497,
		Latitude:  -6.310500077834122,
	})
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Richeese Facotry Hankam",
		Longitude: 106.92429324170865,
		Latitude:  -6.324325887011057,
	})
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Longitude: 106.92405241332614,
		Latitude:  -6.319008375145849,
		Name:      "Test",
	})

	dist := client.GeoDist(ctx, "sellers", "Indomaret Fresh Jatiwarna", "Richeese Facotry Hankam", "km").
		Val()
	assert.Equal(t, float64(1.5379), dist)

	fmt.Println(client.GeoDist(ctx, "sellers", "Test", "Indomaret Fresh Jatiwarna", "km"))

	places := client.GeoSearch(ctx, "sellers", &redis.GeoSearchQuery{
		Longitude:  106.92405241332614,
		Latitude:   -6.319008375145849,
		Radius:     5,
		RadiusUnit: "km",
	}).Val()
	assert.Equal(t, []string{"Indomaret Fresh Jatiwarna", "Richeese Factory Hankam"}, places)
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "characters", "Yukari", "Yukiko", "Rise")
	client.PFAdd(ctx, "characters", "Makoto", "Mitsuru", "Yukari")
	client.PFAdd(ctx, "characters", "Rise", "Fuuka", "Chie")
	assert.Equal(t, int64(7), client.PFCount(ctx, "characters").Val())
}
