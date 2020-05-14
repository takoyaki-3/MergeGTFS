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
)

var wg_unzip sync.WaitGroup

func main() {
	// unzip all gtfs
	paths := dirwalk("./GTFS")
	if err := os.Mkdir("./unzip/", 0777); err != nil {
		fmt.Println(err)
	}
	for index,path := range paths {
		if(!strings.HasSuffix(path, ".zip")){
			continue
		}
		fmt.Println(index)
		fmt.Println(path)
		if err := os.Mkdir("./unzip/"+strconv.Itoa(index), 0777); err != nil {
			fmt.Println(err)
		}
		wg_unzip.Add(1)
		go Unzip(path,"./unzip/"+strconv.Itoa(index))
	}
	wg_unzip.Wait()
	fmt.Println("end")
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