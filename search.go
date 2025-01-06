package vech

import (
	"fmt"
	"math"
	"sort"
)

// SortType
type SortType int

const (
	SortAsc SortType = iota
	SortDesc
)

// Distance represents distance calculation result
type Distance struct {
	N        int     // index number
	Value    float32 // vector distance value
	Position int     // data position
	Size     int     // data size
}

// CosineSim calculates consine simularity over all vectors in collection
// The results can be limited by limit value, 0 means return all
// The results are ordered by sort order
func (c *Collection) CosineSim(vector []float32, sortOrder SortType, limit int) ([]Distance, error) {
	if len(vector) != c.vectorSize {
		return nil, fmt.Errorf("%w: collection vector size: %d, provided vector size: %d", ErrVectorSize, c.vectorSize, len(vector))
	}
	ln := c.Len()
	res := make([]Distance, ln)
	for i := 0; i < ln; i++ {
		irec, _ := c.Index(i)
		res[i] = Distance{
			N:        i,
			Value:    cosineSim(vector, irec.Vector),
			Position: irec.Position,
			Size:     irec.Size,
		}
	}
	if sortOrder == SortAsc {
		sort.Slice(res, func(i, j int) bool {
			return res[i].Value < res[j].Value
		})
	} else {
		sort.Slice(res, func(i, j int) bool {
			return res[i].Value > res[j].Value
		})
	}
	if limit > 0 && len(res) > limit {
		return res[:limit], nil
	}
	return res, nil

}

// assuming the sizes are verified by caller
func cosineSim(a []float32, b []float32) float32 {
	var sa, sb, sab float32 = 0.0, 0.0, 0.0
	for i, va := range a {
		vb := b[i]
		sab += va * vb
		sa += va * va
		sb += vb * vb
	}
	sasb := math.Sqrt(float64(sa)) * math.Sqrt(float64(sb))
	sasb32 := float32(sasb)
	if sasb32 == 0 {
		return 0
	}
	return sab / sasb32
}
