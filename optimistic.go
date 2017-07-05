package optimistic

import (
	"errors"
	"time"

	"github.com/go-redis/redis"
)

type Helper struct {
	redisClient *redis.Client
}

func New(r *redis.Client) *Helper {
	return &Helper{redisClient: r}
}

func (m *Helper) GetRecord(key string) (content string, originalTime time.Time, found bool) {
	result, err := m.redisClient.HGetAll(key).Result()
	if err == redis.Nil || len(result) == 0 {
		return ``, time.Time{}, false
	}
	if err != nil {
		//unknown error
		panic(err)
	}
	s1, ok1 := result[`content`]
	s2, ok2 := result[`ts`]
	if ok1 == false || ok2 == false {
		panic(errors.New(`The value in redis mismatch`))
	}
	originalTime, err = time.Parse(time.RFC3339Nano, s2)
	if err != nil {
		panic(errors.New(`The "ts" field in optimisticRecord is not valid time.`))
	}

	return s1, originalTime, true
}

func (m *Helper) Update(key, content string, originalTime, currentTime time.Time) (updateOK bool) {
	//FIXME: use evalsha instead of direct scripting
	script := `
if redis.call('EXISTS', KEYS[1]) == 1 then
	if redis.call('HGET', KEYS[1], 'ts') ~= KEYS[3] then
		return 0
	end 
end 
redis.call('HMSET', KEYS[1], 'content', KEYS[2], 'ts', KEYS[4]) 
return 1
`
	oriTs, currTs := originalTime.UTC().Format(time.RFC3339Nano), currentTime.UTC().Format(time.RFC3339Nano)
	result, err := m.redisClient.Eval(script, []string{key, content, oriTs, currTs}).Result()
	if err != nil {
		panic(err)
	}

	switch t := result.(type) {
	case string:
		return t == `1`
	case int64:
		return t == 1
	default:
		//FIXME: should reach this line
		panic(errors.New(`Unknown return from redis eval.`))
	}

	return false
}
