package collections

const (
	defaultCompactMapMinPeak       = 1024
	defaultCompactMapSparseDivisor = 4
	defaultCompactMapEmptyResetCap = 64
)

type CompactMapOptions struct {
	RebuildMinPeak  int
	SparseShrinkDiv int
	EmptyResetCap   int
}

type CompactMap[K comparable, V any] struct {
	data map[K]V
	peak int
	opts CompactMapOptions
}

func NewCompactMap[K comparable, V any](initialCap int, opts CompactMapOptions) *CompactMap[K, V] {
	if initialCap < 0 {
		initialCap = 0
	}
	opts = normalizeCompactMapOptions(opts)
	return &CompactMap[K, V]{
		data: make(map[K]V, initialCap),
		opts: opts,
	}
}

func (m *CompactMap[K, V]) Set(key K, value V) {
	m.data[key] = value
	if n := len(m.data); n > m.peak {
		m.peak = n
	}
}

func (m *CompactMap[K, V]) Get(key K) (V, bool) {
	value, ok := m.data[key]
	return value, ok
}

func (m *CompactMap[K, V]) Delete(key K) {
	delete(m.data, key)
}

func (m *CompactMap[K, V]) Len() int {
	return len(m.data)
}

func (m *CompactMap[K, V]) Peak() int {
	return m.peak
}

func (m *CompactMap[K, V]) MaybeCompact() bool {
	if m.peak < m.opts.RebuildMinPeak {
		return false
	}
	if len(m.data) == 0 {
		m.data = make(map[K]V, m.opts.EmptyResetCap)
		m.peak = 0
		return true
	}
	if len(m.data)*m.opts.SparseShrinkDiv >= m.peak {
		return false
	}

	rebuilt := make(map[K]V, len(m.data))
	for key, value := range m.data {
		rebuilt[key] = value
	}
	m.data = rebuilt
	m.peak = len(m.data)
	return true
}

func normalizeCompactMapOptions(opts CompactMapOptions) CompactMapOptions {
	if opts.RebuildMinPeak <= 0 {
		opts.RebuildMinPeak = defaultCompactMapMinPeak
	}
	if opts.SparseShrinkDiv <= 1 {
		opts.SparseShrinkDiv = defaultCompactMapSparseDivisor
	}
	if opts.EmptyResetCap < 0 {
		opts.EmptyResetCap = 0
	}
	if opts.EmptyResetCap == 0 {
		opts.EmptyResetCap = defaultCompactMapEmptyResetCap
	}
	return opts
}
