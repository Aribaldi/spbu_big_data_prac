package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
)

const numbersCount = 2000


func PrimeFactors(n uint32) int {
	res := 0

	for n%2 == 0 {
		res++
		n = n / 2
	}


	i := uint64(3)
	for ; i*i <= uint64(n); i = i + 2 {

		for uint64(n)%i == 0 {
			res++
			n = n / uint32(i)
		}
	}


	if n > 2 {
		res++
	}

	return res
}

func factorize(in chan uint32, out chan int) {
	for num := range in {
		out <- PrimeFactors(num)
	}
}

func readFile() (res []uint32) {
	rf, err := os.Open("numbers.txt")
	if err != nil {
		panic(err)
	}
	defer rf.Close()

	fs := bufio.NewScanner(rf)
	fs.Split(bufio.ScanLines)
	for fs.Scan() {
		num, err := strconv.ParseUint(fs.Text(), 10, 32)
		if err != nil {
			panic(err)
		}
		res = append(res, uint32(num))
	}
	return
}

func sender(in chan uint32, nums []uint32) {
	defer close(in)
	for _, num := range nums {
		in <- num
	}
}

func main() {
	goroutinesCount := 4
	in := make(chan uint32, numbersCount)
	out := make(chan int, numbersCount)
	nums := readFile()

	start := time.Now()

	go sender(in, nums)
	for i := 0; i < goroutinesCount; i++ {
		go factorize(in, out)
	}

	res := 0
	for i := 0; i < numbersCount; i++ {
		res += <-out
	}

	duration := time.Since(start)
	fmt.Println("Execution time: ", duration)

	fmt.Println("Answer: ", res)
}
