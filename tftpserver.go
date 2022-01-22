package main

import (
    "fmt"
    "net"
    "os"
    "io"
    "bytes"
    "encoding/binary"
    "encoding/json"
    //"time"
)

const (
    //maxBufferSize = 1024
    maxBufferSize = 512

    EOF = 0
    OPCODE_RRQ = 1
    OPCODE_WRQ = 2
    OPCODE_DATA = 3
    OPCODE_ACK = 4
    OPCODE_ERROR = 5
)

type Request struct {}


func (r *Request) newRequest(config Configuration, n int, addr net.Addr, buffer []byte){
    opcode := parseOpcode(buffer[0:2])
    if opcode == OPCODE_RRQ {
        c := ReadRequestHandler{}
        c.remote = addr
        c.timeout = 60
        c.blocksize = maxBufferSize
        c.blocknumber = 1
        r.parse_rrq(&c, buffer)
        c.state = OPCODE_DATA
        c.process()
    } else {
        fmt.Printf("ERROR: received opcode %d out of sequence", opcode)
    }

    return
}


func (r *Request) parse_rrq(conn *ReadRequestHandler, buffer []byte) error {

    fields := bytes.Split(buffer[2:], []byte {0})
    conn.file = string(fields[0])
    conn.mode = string(fields[1])

    conn.options = make(map[string]string)
    x := 5
    for len(fields[x]) != 0 {
        fmt.Printf("%s = %s\n",string(fields[x]) , string(fields[x]))
        conn.options[string(fields[x])] = string(fields[x])
        x+=2
    }

    return nil
}



///////////////////////////////////////////////////////////
type ReadRequestHandler struct {
    remote net.Addr
    file string
    mode string
    state uint
    timeout int64
    options map[string]string
    blocknumber int
    blocksize int
    lastblocksize int
}


func (rrq *ReadRequestHandler) process(){

    pc, err := net.ListenPacket("udp", "0.0.0.0:0")
    if err != nil {
        fmt.Printf("err=%s\n", err)
        return
    }
    defer pc.Close()

    //deadline := time.Now().Add(6000000)
    //err = pc.SetReadDeadline(deadline)
    //if err != nil {
    //    fmt.Printf("ERROR: failed to set read timeout: %s\n", err)
    //}

    for {
        // send data

        if rrq.state == OPCODE_DATA {
            //fmt.Printf("state = OPCODE_DATA\n")
            var pckt []byte
            pckt, err := rrq.format_data_packet()
            if err != nil  {
                pckt = rrq.format_err_packet(err)
                fmt.Printf("ERROR1: %s\n", err)
                rrq.state = EOF 
            }
            n, err := pc.WriteTo(pckt, rrq.remote)
            fmt.Printf("%s sent block %d (%d bytes)\n",rrq.remote.String(), rrq.blocknumber, n)
            if err == nil && n == (rrq.lastblocksize+4) && rrq.state != EOF {
                rrq.state = OPCODE_ACK 
            }else{
                fmt.Printf("ERROR2: %s pckt=%s rrq=%+v\n", err, pckt, rrq)
                panic(0)
            }

        }else if rrq.state == OPCODE_ACK {
            //fmt.Printf("state = OPCODE_ACK\n")

            buffer := make([]byte, rrq.blocksize)
            n, _, err := pc.ReadFrom(buffer)
            if err == nil {
                rrq.parse_ack(n, buffer)
            }else {
                rrq.state = OPCODE_DATA
            }
        }else if rrq.state == EOF {
            break
        }

    }
    fmt.Println("end")

}


func (rrq *ReadRequestHandler) parse_ack(n int, buffer []byte) bool {
    if n < 4 {
        // packet is weirdly short
        return true
    }
    opcode :=  parseOpcode(buffer[0:2])
    if opcode != OPCODE_ACK {
        fmt.Printf("Expected opcode %d got %d\n", OPCODE_ACK, opcode)
        return true
    } 

    var blockno int = parseOpcode(buffer[2:])

    if blockno == rrq.blocknumber {
        rrq.blocknumber = rrq.blocknumber + 1
        fmt.Printf("ACKed block %d\n",blockno)
        if  rrq.lastblocksize < rrq.blocksize {
            //we've sent the last of the data
            rrq.state = EOF
        }

        return false
    }

    rrq.state = OPCODE_DATA 

    return true
}


func(rrq *ReadRequestHandler) format_err_packet(e error) []byte {
    buffer := make([]byte, len(e.Error())+2)

    binary.BigEndian.PutUint16(buffer, uint16(OPCODE_ERROR))

    copy(buffer[2:], e.Error())

    return buffer
}


func (rrq *ReadRequestHandler) format_data_packet() ([]byte, error) {
    buffer := make([]byte, rrq.blocksize +4)

    binary.BigEndian.PutUint16(buffer, uint16(OPCODE_DATA))
    binary.BigEndian.PutUint16(buffer[2:], uint16(rrq.blocknumber))

    bufferlen, err := rrq.read_file(buffer[4:]) 
    if err != nil {
        //conn.errormsg = err.Error()
        return nil, err
    }
    rrq.lastblocksize = bufferlen

    return buffer[:bufferlen+4], nil

}


func (rrq *ReadRequestHandler) read_file(buffer []byte) (int, error) {

    f, err := os.Open(rrq.file)
    if err != nil {
        fmt.Printf("ERROR: %s\n",err)
        return -1,err
    }

    defer f.Close()
    //fmt.Println(conn)
    fileoffset := (rrq.blocknumber-1) * rrq.blocksize
    fmt.Printf("read %d from offset %d\n", len(buffer), fileoffset)
    bufferlen, err := f.ReadAt(buffer, int64(fileoffset))
    if err != nil {
        if err == io.EOF {
            fmt.Printf("EOF happened\n")
        }else{
            fmt.Printf("ERROR: %s\n",err)
            return -1,err
        }
    }
    //fmt.Printf("read buffer=%s\n",buffer)
    return bufferlen,nil
}

///////////////////////////////////////////////////////////

func parseOpcode(buffer []byte) int {

    code := int(buffer[0])
    code = (code << 8 | int(buffer[1]))
    //fmt.Printf("opcode=%d\n",code)
    return code
}

type Configuration struct {
    Ip    string
    Port  string
    Dir   string
}


func main(){

    file, err := os.Open("/etc/tftpserver.json")
    if err != nil {
        file, err = os.Open("./tftpserver.json")
        if err != nil {
            fmt.Println("unable to open /etc/tftpserver.json or ./tftpserver.json for config")
            os.Exit(1)
        }
    }
    defer file.Close()
    
    decoder := json.NewDecoder(file)
    config := Configuration{}
    err = decoder.Decode(&config)
    if err != nil {
      fmt.Println("error:", err)
    }
    fmt.Printf("config=%+v\n", config)
    
    fmt.Printf("==== start listening on %s:%s====\n", config.Ip ,  config.Port)
    //pc, err := net.ListenPacket("udp", "127.0.0.1:12345")
    pc, err := net.ListenPacket("udp", config.Ip + ":" + config.Port)
    if err != nil {
        fmt.Println("ERROR: %s\n",err)
    }
    for {
        buffer := make([]byte, 4096)
        fmt.Println("==== read from ===")
        n, addr, err := pc.ReadFrom(buffer)
        if err != nil {
            fmt.Println("failed to read from buffer: %s", err)
        }
        fmt.Printf("connection from: %s\n",addr)
        r := Request{}
        go r.newRequest(config, n, addr, buffer)
    }
}
