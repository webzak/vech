package vech

import "testing"

func TestCosineSim(t *testing.T) {
	opt := CreateDbOptions{
		VectorSize:  4,
		StorageType: Memory,
	}
	db, err := CreateDb(&opt)
	if err != nil {
		t.Fatal(err)
	}
	c, err := db.OpenCollection("foo")
	if err != nil {
		t.Fatal(err)
	}
	err = addChunks(c, chunks)
	if err != nil {
		t.Fatal(err)
	}

	vector := []float32{0.3, 0.8, 0.333, 4.3}

	dist, err := c.CosineSim(vector, SortAsc, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dist) != 4 {
		t.Fatalf("result length is expected to be 4, actual: %d", len(dist))
	}

	for _, d := range dist {
		if d.Value < -1.0 || d.Value > 1.0 {
			t.Fatalf("the value is out of range -1:1 %f", d.Value)
		}
	}

	dist, err = c.CosineSim(vector, SortDesc, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(dist) != 3 {
		t.Fatalf("result length is expected to be 3, actual: %d", len(dist))
	}
	if dist[0].Value < dist[1].Value || dist[0].Value < dist[2].Value || dist[1].Value < dist[2].Value {
		t.Fatal("The order is expected to be descend")
	}

	dist, err = c.CosineSim(vector, SortAsc, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(dist) != 3 {
		t.Fatalf("result length is expected to be 3, actual: %d", len(dist))
	}
	if dist[0].Value > dist[1].Value || dist[0].Value > dist[2].Value || dist[1].Value > dist[2].Value {
		t.Fatal("The order is expected to be ascend")
	}
}
