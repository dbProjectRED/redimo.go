package redimo

type Side string

const (
	Before Side = "BEFORE"
	After  Side = "AFTER"
)

func (c Client) LINDEX(key string, index int64) (element string, err error) {
	return
}

func (c Client) LINSERT(key string, side Side, pivotElement string) (newLength int64, err error) {
	return
}

func (c Client) LLEN(key string) (length int64, err error) {
	return
}

func (c Client) LPOP(key string) (element string, ok bool, err error) {
	return
}

func (c Client) LPUSH(key string, elements ...string) (newLength int64, err error) {
	return
}

func (c Client) LPUSHX(key string, elements ...string) (newLength int64, err error) {
	return
}

func (c Client) LRANGE(key string, start, stop int64) (elements []string, err error) {
	return
}

func (c Client) LREM(key string, count int64, element string) (removedCount int64, err error) {
	return
}

func (c Client) LSET(key string, index int64, element string) (ok bool, err error) {
	return
}

func (c Client) LTRIM(key string, start, stop int64) (ok bool, err error) {
	return
}

func (c Client) RPOP(key string) (element string, ok bool, err error) {
	return
}

func (c Client) RPOPLPUSH(sourceKey string, destinationKey string) (element string, ok bool, err error) {
	return
}

func (c Client) RPUSH(key string, elements ...string) (newLength int64, err error) {
	return
}

func (c Client) RPUSHX(key string, elements ...string) (newLength int64, err error) {
	return
}
