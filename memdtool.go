package memdtool

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	exitCodeOK = iota
	exitCodeParseFlagErr
	exitCodeErr
)

// CLI is struct for command line tool
type CLI struct {
	OutStream, ErrStream io.Writer
}

type MemKeyExp struct {
	Key string
	Exp int64
}

var helpReg = regexp.MustCompile(`^--?h(?:elp)?$`)

// Run the memdtool
func (cli *CLI) Run(argv []string) int {
	log.SetOutput(cli.ErrStream)
	log.SetFlags(0)

	mode := "display"
	addr := "127.0.0.1:11211"
	if len(argv) > 0 {
		modeCandidate := argv[len(argv)-1]
		if modeCandidate == "display" || modeCandidate == "dump" {
			mode = modeCandidate
			argv = argv[:len(argv)-1]
		}
		if len(argv) > 0 {
			addr = argv[0]
			if helpReg.MatchString(addr) {
				printHelp(cli.ErrStream)
				return exitCodeOK
			}
		}
	}

	var proto = "tcp"
	if strings.Contains(addr, "/") {
		proto = "unix"
	}

	switch mode {
	case "display":
		// create conn
		conn, connErr := cli.dialer(proto, addr)()
		if connErr != nil {
			log.Println(connErr.Error())
			return exitCodeErr
		}
		return cli.display(conn)
	case "dump":
		// create keyConn
		keyConn, keyConnErr := cli.dialer(proto, addr)()
		if keyConnErr != nil {
			log.Println(keyConnErr.Error())
			return exitCodeErr
		}
		// create valConn
		valConn, valConnErr := cli.dialer(proto, addr)()
		if valConnErr != nil {
			log.Println(valConnErr.Error())
			return exitCodeErr
		}
		return cli.dump(keyConn, valConn, cli.dialer(proto, addr))
	}
	return exitCodeErr
}

func (cli *CLI) display(conn net.Conn) int {

	curConn := conn
	defer curConn.Close()

	items, err := GetSlabStats(conn)
	if err != nil {
		log.Println(err.Error())
		return exitCodeErr
	}

	fmt.Fprint(cli.OutStream, "  #  Item_Size  Max_age   Pages   Count   Full?  Evicted Evict_Time OOM\n")
	for _, ss := range items {
		if ss.TotalPages == 0 {
			continue
		}
		size := fmt.Sprintf("%dB", ss.ChunkSize)
		if ss.ChunkSize > 1024 {
			size = fmt.Sprintf("%.1fK", float64(ss.ChunkSize)/1024.0)
		}
		full := "no"
		if ss.FreeChunksEnd == 0 {
			full = "yes"
		}
		fmt.Fprintf(cli.OutStream,
			"%3d %8s %9ds %7d %7d %7s %8d %8d %4d\n",
			ss.ID,
			size,
			ss.Age,
			ss.TotalPages,
			ss.Number,
			full,
			ss.Evicted,
			ss.EvictedTime,
			ss.Outofmemory,
		)
	}
	return exitCodeOK
}

func (cli *CLI) watchErr(errChan chan int, finChan chan struct{}) {
	ecode := <-errChan
	close(finChan)
	cli.setErr(ecode, errChan)
}

func (cli *CLI) setErr(errCode int, errChan chan int) {
	select {
	case errChan <- errCode:
		/* ignore */
	default:
		/* ignore */
	}
}

func (cli *CLI) isClosedFinChan(finChan chan struct{}) bool {
	select {
	case <-finChan:
		return true
	default:
		return false
	}
}

func (cli *CLI) closeFinChan(finChan chan struct{}) {
	if !cli.isClosedFinChan(finChan) {
		close(finChan)
	}
}

func (cli *CLI) dialer(proto string, addr string) func() (net.Conn, error) {
	return func() (net.Conn, error) {
		// get connection
		nc, err := net.Dial(proto, addr)
		if err != nil {
			return nil, err
		}

		// set keep-alive
		if _, ok := nc.(*net.TCPConn); ok {
			err = nc.(*net.TCPConn).SetKeepAlive(true)
			if err != nil {
				nc.Close()
				return nil, err
			}
			err = nc.(*net.TCPConn).SetKeepAlivePeriod(time.Duration(10) * time.Second)
			if err != nil {
				nc.Close()
				return nil, err
			}
		}
		return nc, nil
	}
}

func (cli *CLI) getKeys(conn net.Conn, keyChan chan MemKeyExp, errChan chan int, finChan chan struct{}, wg *sync.WaitGroup) {

	curConn := conn
	defer curConn.Close()

	// close finChan
	// done waitgroup
	defer func(finChan chan struct{}, wg *sync.WaitGroup) {
		cli.closeFinChan(finChan)
		wg.Done()
	}(finChan, wg)

	// get total number of items
	items, itemsErr := GetSlabStats(conn)
	if itemsErr != nil {
		log.Println(itemsErr.Error())
		cli.setErr(exitCodeErr, errChan)
		return
	}
	var itemCnt, itemLimit uint64
	for _, item := range items {
		itemLimit += item.Number
	}

	// get keys
	rdr, wtr, isBusy := bufio.NewReader(conn), bufio.NewWriter(conn), true
	for isBusy {
		// check limit
		if cli.isClosedFinChan(finChan) || (itemCnt >= itemLimit) {
			break
		}

		// get all keys from lru_crawler metadump all
		fmt.Fprint(wtr, "lru_crawler metadump all\r\n")
		wtr.Flush()
		for {
			// check limit
			if cli.isClosedFinChan(finChan) || (itemCnt >= itemLimit) {
				break
			}

			// get line
			lineBytes, _, lineBytesErr := rdr.ReadLine()
			if lineBytesErr != nil {
				log.Printf(lineBytesErr.Error())
				cli.setErr(exitCodeErr, errChan)
				return
			}
			line := string(lineBytes)
			if strings.HasPrefix(line, "BUSY currently processing crawler request") {
				log.Printf("BUSY currently processing crawler request wait 5sec")
				time.Sleep(time.Duration(5) * time.Second)
				break
			} else if isBusy {
				log.Printf("lru_crawler dump start")
				isBusy = false
			}
			if line == "END" {
				break
			}
			if !strings.Contains(line, "key=") {
				continue
			}

			// [format]
			// key=UID%3A%3A11%3A%3ACAESEHQTlN-G6eC5F1E7i4JFC_s exp=1610365183 la=1607674998 cas=2522 fetch=yes
			arr1 := strings.Split(line, " ")
			if len(arr1) < 2 || arr1[0] == "" || arr1[1] == "" {
				continue
			}
			keyval1 := strings.Split(arr1[0], "=")
			if len(keyval1) < 2 || keyval1[1] == "" {
				continue
			}
			keyval2 := strings.Split(arr1[1], "=")
			if len(keyval2) < 2 || keyval2[1] == "" {
				continue
			}
			exp64, exp64_err := strconv.ParseInt(keyval2[1], 10, 64)
			if exp64_err != nil {
				continue
			}
			decoded_key, _ := url.QueryUnescape(keyval1[1])
			if time.Now().Unix() > exp64 {
				log.Printf("expired=%s", decoded_key)
				continue
			}
			keyChan <- MemKeyExp{Key: decoded_key, Exp: exp64}
			itemCnt++
		}
	}
}

func (cli *CLI) dumpData(conn net.Conn, keyChan chan MemKeyExp, errChan chan int, finChan chan struct{}, wg *sync.WaitGroup) {

	curConn := conn
	defer curConn.Close()

	// done waitgroup
	defer func(wg *sync.WaitGroup) {
		wg.Done()
	}(wg)

	// get key-value pairs
	rdr := bufio.NewReader(conn)
	wtrs := bufio.NewWriter(conn)
	wtro := bufio.NewWriter(cli.OutStream)
	for {
		// finish dump
		if cli.isClosedFinChan(finChan) && len(keyChan) == 0 {
			break
		}
		// get key from keyChan
		keyExp := <-keyChan
		cachekey, exp := keyExp.Key, keyExp.Exp
		fmt.Fprintf(wtrs, "get %s\r\n", cachekey)
		wtrs.Flush()
		for {
			lineBytes, _, lineBytesErr := rdr.ReadLine()
			if lineBytesErr != nil {
				log.Println(lineBytesErr.Error())
				cli.setErr(exitCodeErr, errChan)
				return
			}
			line := string(lineBytes)
			if line == "END" {
				break
			}
			// VALUE hoge 0 6
			// hogege
			fields := strings.Fields(line)
			if len(fields) != 4 || fields[0] != "VALUE" {
				continue
			}
			flags := fields[2]
			sizeStr := fields[3]
			size, _ := strconv.Atoi(sizeStr)
			buf := make([]byte, size)
			_, readErr := rdr.Read(buf)
			if readErr != nil {
				log.Println(readErr.Error())
				cli.setErr(exitCodeErr, errChan)
				return
			}
			fmt.Fprintf(wtro, "add %s %s %d %s\r\n%s\r\n", cachekey, flags, exp, sizeStr, string(buf))
			rdr.ReadLine()
		}
	}
	wtro.Flush()
}

func (cli *CLI) dump(keyConn net.Conn, valConn net.Conn, dialer func() (net.Conn, error)) int {
	log.Printf("dump start")

	// init channels
	wg := sync.WaitGroup{}
	keyChan := make(chan MemKeyExp, 100000)
	errChan := make(chan int, 1)
	finChan := make(chan struct{}, 1)

	// watch errors
	go cli.watchErr(errChan, finChan)
	// get keys
	wg.Add(1)
	go cli.getKeys(keyConn, keyChan, errChan, finChan, &wg)
	// dump data
	wg.Add(1)
	go cli.dumpData(valConn, keyChan, errChan, finChan, &wg)

	// wait
	<-finChan
	wg.Wait()
	log.Printf("dump finished.")

	// return exit code
	select {
	case ecode := <-errChan:
		return ecode
	default:
		return exitCodeOK
	}
}

func printHelp(w io.Writer) {
	fmt.Fprintf(w, `Usage: memcached-tool <host[:port] | /path/to/socket> [mode]

       memcached-tool 127.0.0.1:11211 display # shows slabs
       memcached-tool 127.0.0.1:11211         # same. (default is display)
       memcached-tool 127.0.0.1:11211 dump    # dump keys and values

Version: %s (rev: %s)
`, version, revision)
}

// SlabStat represents slab statuses
type SlabStat struct {
	ID             uint64
	Number         uint64 // Count?
	Age            uint64
	Evicted        uint64
	EvictedNonzero uint64
	EvictedTime    uint64
	Outofmemory    uint64
	Reclaimed      uint64
	ChunkSize      uint64
	ChunksPerPage  uint64
	TotalPages     uint64
	TotalChunks    uint64
	UsedChunks     uint64
	FreeChunks     uint64
	FreeChunksEnd  uint64
}

// GetSlabStats takes SlabStats from connection
func GetSlabStats(conn net.Conn) ([]*SlabStat, error) {
	retMap := make(map[int]*SlabStat)
	fmt.Fprint(conn, "stats items\r\n")
	scr := bufio.NewScanner(bufio.NewReader(conn))
	for scr.Scan() {
		// ex. STAT items:1:number 1
		line := scr.Text()
		if line == "END" {
			break
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("result of `stats items` is strange: %s", line)
		}
		fields2 := strings.Split(fields[1], ":")
		if len(fields2) != 3 {
			return nil, fmt.Errorf("result of `stats items` is strange: %s", line)
		}
		key := fields2[2]
		slabNum, _ := strconv.ParseUint(fields2[1], 10, 64)
		value, _ := strconv.ParseUint(fields[2], 10, 64)
		ss, ok := retMap[int(slabNum)]
		if !ok {
			ss = &SlabStat{ID: slabNum}
			retMap[int(slabNum)] = ss
		}
		switch key {
		case "number":
			ss.Number = value
		case "age":
			ss.Age = value
		case "evicted":
			ss.Evicted = value
		case "evicted_nonzero":
			ss.EvictedNonzero = value
		case "evicted_time":
			ss.EvictedNonzero = value
		case "outofmemory":
			ss.Outofmemory = value
		case "reclaimed":
			ss.Reclaimed = value
		}
	}
	if err := scr.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to GetSlabStats while scaning stats items")
	}

	fmt.Fprint(conn, "stats slabs\r\n")
	for scr.Scan() {
		// ex. STAT 1:chunk_size 96
		line := scr.Text()
		if line == "END" {
			break
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("result of `stats slabs` is strange: %s", line)
		}
		fields2 := strings.Split(fields[1], ":")
		if len(fields2) != 2 {
			continue
		}
		key := fields2[1]
		slabNum, _ := strconv.ParseUint(fields2[0], 10, 64)
		value, _ := strconv.ParseUint(fields[2], 10, 64)
		ss, ok := retMap[int(slabNum)]
		if !ok {
			ss = &SlabStat{}
			retMap[int(slabNum)] = ss
		}

		switch key {
		case "chunk_size":
			ss.ChunkSize = value
		case "chunks_per_page":
			ss.ChunksPerPage = value
		case "total_pages":
			ss.TotalPages = value
		case "total_chunks":
			ss.TotalChunks = value
		case "used_chunks":
			ss.UsedChunks = value
		case "free_chunks":
			ss.FreeChunks = value
		case "free_chunks_end":
			ss.FreeChunksEnd = value
		}
	}
	if err := scr.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to GetSlabStats while scaning stats slabs")
	}

	keys := make([]int, 0, len(retMap))
	for i := range retMap {
		keys = append(keys, i)
	}
	sort.Ints(keys)
	ret := make([]*SlabStat, len(keys))
	for i, v := range keys {
		ret[i] = retMap[v]
	}
	return ret, nil
}
