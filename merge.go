package main

import (
    "fmt"
    "io/ioutil"
		"path/filepath"
		"archive/zip"
		"io"
		"os"
		"strconv"
		"strings"
		"sync"
		"encoding/csv"
)

var wg_unzip sync.WaitGroup
var num_of_gtfs int

func main() {
	// initi
	num_of_gtfs = 0

	// unzip all gtfs
	paths := dirwalk("./GTFS")
	if err := os.Mkdir("./unzip/", 0777); err != nil {
		fmt.Println(err)
	}
	for index,path := range paths {
		if(!strings.HasSuffix(path, ".zip")){
			continue
		}
//		fmt.Println(index)
		fmt.Println(path)
		if err := os.Mkdir("./unzip/"+strconv.Itoa(index), 0777); err != nil {
			fmt.Println(err)
		}
		wg_unzip.Add(1)
		go Unzip(path,"./unzip/"+strconv.Itoa(index))
		num_of_gtfs++
	}
	wg_unzip.Wait()

	// load csv
	for i:=0;i<num_of_gtfs;i++ {
		head,data := load_gtfs_file(i,"agency")

		fmt.Println(head)
		fmt.Println(data)
	}
	fmt.Println("end")
}

func load_gtfs_file(index int,filename string) (head map[string]int,records [][]string) {

	head = map[string]int{}
	records = [][]string{}

	file, err := os.Open("./unzip/"+strconv.Itoa(index)+"/"+filename+".txt")
	if err != nil {
			panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	isf := true
	for {
		var line []string
		line, err = reader.Read()
		if err != nil {
				break
		}
		if isf {
			for ind,str := range line {
				head[str] = ind
			}
			isf=false
			continue
		}

		records = append(records,line)
	}

	return
}

func dirwalk(dir string) []string {
    files, err := ioutil.ReadDir(dir)
    if err != nil {
        panic(err)
    }

    var paths []string
    for _, file := range files {
        if file.IsDir() {
            paths = append(paths, dirwalk(filepath.Join(dir, file.Name()))...)
            continue
        }
        paths = append(paths, filepath.Join(dir, file.Name()))
    }

    return paths
}

func Unzip(src, dest string) error {
	defer wg_unzip.Done()
	r, err := zip.OpenReader(src)
	if err != nil {
			return err
	}
	defer r.Close()

	for _, f := range r.File {
			rc, err := f.Open()
			if err != nil {
					return err
			}
			defer rc.Close()

			path := filepath.Join(dest, f.Name)
			if f.FileInfo().IsDir() {
					os.MkdirAll(path, f.Mode())
			} else {
					f, err := os.OpenFile(
							path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
					if err != nil {
							return err
					}
					defer f.Close()

					_, err = io.Copy(f, rc)
					if err != nil {
							return err
					}
			}
	}

	return nil
}