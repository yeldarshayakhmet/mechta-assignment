package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

type Model struct {
	A int
	B int
}

// parallelSplitSum sums elements in a slice chunk
// Sum of every field is pushed to its dedicated channel
func parallelSplitSum(data []Model, sumA, sumB chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	var a, b int
	for _, v := range data {
		a += v.A
		b += v.B
	}
	sumA <- a
	sumB <- b
}

// parallelSum sums every K-th element of a slice, where K is the number of goroutines (step)
// This is a different approach but results in poorer performance
func parallelSum(offset int, data []Model, sumA, sumB chan<- int, step int, wg *sync.WaitGroup) {
	defer wg.Done()
	var a, b int
	for i := offset; i < len(data); i += step {
		a += data[i].A
		b += data[i].B
	}
	sumA <- a
	sumB <- b
}

func main() {
	// Parse number of goroutines
	nGoroutines, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid argument", err)
		return
	}

	jsonData, err := os.ReadFile("data.json")
	if err != nil {
		fmt.Println("Error reading input file", err)
	}

	var data []Model
	err = json.Unmarshal(jsonData, &data)
	if err != nil {
		fmt.Println("Error parsing json", err)
	}

	// Buffered channel for every field so the sums can be pushed without blocking
	var wg sync.WaitGroup
	sumA := make(chan int, nGoroutines)
	sumB := make(chan int, nGoroutines)

	startTime := time.Now()
	chunkSize := (len(data) + nGoroutines - 1) / nGoroutines
	for i := 0; i < nGoroutines; i++ {
		wg.Add(1)
		//go parallelSum(i, data, sumA, sumB, n_goroutines, &wg)
		// Identify the next slice chunk
		start := i * chunkSize
		end := start + chunkSize
		go parallelSplitSum(data[start:end], sumA, sumB, &wg)
	}

	go func() {
		wg.Wait()
		close(sumA)
		close(sumB)
	}()

	var totalA, totalB int
	for v := range sumA {
		totalA += v
	}
	for v := range sumB {
		totalB += v
	}
	fmt.Println(time.Since(startTime))

	fmt.Printf("Sum of a: %d\n", totalA)
	fmt.Printf("Sum of b: %d\n", totalB)
}