package main

import (
	"log"

	"github.com/YOwatari/parquet-go/ParquetFile"
	"github.com/YOwatari/parquet-go/ParquetReader"
	"github.com/YOwatari/parquet-go/ParquetWriter"
	"github.com/YOwatari/parquet-go/parquet"
)

type Student struct {
	Name
	ID
	Age int64 `parquet:"name=age, Type=INT64"`
}

type Name struct {
	Name    string  `parquet:"name=name, type=UTF8, encoding=PLAIN_DICTIONARY"`
}

type ID struct {
	ID      int64   `parquet:"name=id, type=INT64"`
}

func main() {
	var err error
	fw, err := ParquetFile.NewLocalFileWriter("flat.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	//write
	pw, err := ParquetWriter.NewParquetWriter(fw, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 100
	for i := 0; i < num; i++ {
		stu := Student{
			Name{"StudentName"},
			ID{     int64(i)},
			int64(i),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	fw.Close()

	///read
	fr, err := ParquetFile.NewLocalFileReader("flat.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := ParquetReader.NewParquetReader(fr, new(Student), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			pr.SkipRows(10) //skip 10 rows
			continue
		}
		stus := make([]Student, 10) //read 10 rows
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}

	pr.ReadStop()
	fr.Close()

}
