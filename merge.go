package main

import (
    "fmt"
    "io/ioutil"
		"path/filepath"
		"archive/zip"
		"io"
		"os"
		"strconv"
)

func main() {
	paths := dirwalk("./GTFS")
	if err := os.Mkdir("./unzip/", 0777); err != nil {
		fmt.Println(err)
	}
	for index,path := range paths {
		fmt.Println(index)
		fmt.Println(path)
		if err := os.Mkdir("./unzip/"+strconv.Itoa(index), 0777); err != nil {
			fmt.Println(err)
		}
		Unzip(path,"./unzip/"+strconv.Itoa(index))
	}
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