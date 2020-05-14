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

var wg_unzip,wg_load sync.WaitGroup
var num_of_gtfs int

/*
type agency struct {
	agency_id string
	agency_name	string
	agency_url string
	agency_timezone string
	agency_lang string
	agency_phone string
	agency_fare_url string
	agency_email string
}

type stops struct {
	stop_id string
	stop_code	string
	stop_name string
	stop_desc string
	stop_lat string
	stop_lon string
	zone_id string
	stop_url string
	location_type
	parent_station
	stop_timezone
	wheelchair_boarding
}*/

type gtfs_csv struct{
	head map[string]int
	data [][]string
/*	agency [][]string
	agency_head map[string]int
	stops [][]string
	stops_head map[string]int
	routes [][]string
	routes_head map[string]int
	trips [][]string
	trips_head map[string]int
	stop_times [][]string
	stop_times_head map[string]int
	calendar [][]string
	calendar_head map[string]int
	calendar_dates [][]string
	calendar_dates_head map[string]int
	fare_attributes [][]string
	fare_attributes_head map[string]int
	shapes [][]string
	shapes_head map[string]int
	frequencies [][]string
	frequencies_head map[string]int
	transfers [][]string
	transfers_head map[string]int
	pathways [][]string
	pathways_head map[string]int
	levels [][]string
	levels_head map[string]int
	feed_info [][]string
	feed_info_head map[string]int
	translations [][]string
	translations_head map[string]int
	attributions [][]string
	attributions_head map[string]int*/
}

func merge_head(head1 map[string]int,head2 map[string]int)(head map[string]int){
	head = map[string]int{}
	i := 0
	for str,_ := range head1 {
		if _, ok := head1[str]; !ok{
			head[str] = i
			i++	
		}
	}
	return
}

type gtfs_type map[string]gtfs_csv

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

	// load GTFS CSV
	var gtfss map[int]gtfs_type
	gtfss = map[int]gtfs_type{}
	for i:=0;i<num_of_gtfs;i++ {
		wg_load.Add(1)
		go func(i int){
			defer wg_load.Done()
			csv_files := dirwalk("./unzip/"+strconv.Itoa(i)+"/")
			files := gtfs_type{}

			for _,file_name := range csv_files{
				var csv_file gtfs_csv
				csv_file.head,csv_file.data = load_gtfs_file(i,file_name)
				files[file_name] = csv_file
			}
			gtfss[i]=files
		}(i)
	}
	wg_load.Wait()

	// 

	fmt.Println(gtfss)
/*	for i:=0;i<num_of_gtfs;i++ {
		var gtfs gtfs_csv
		gtfs.agency_head,gtfs.agency = load_gtfs_file(i,"agency")
		gtfs.stops_head,gtfs.stops = load_gtfs_file(i,"stops")
		gtfs.routes_head,gtfs.routes = load_gtfs_file(i,"routes")
		gtfs.agency_head,gtfs.agency = load_gtfs_file(i,"agency")
		gtfs.agency_head,gtfs.agency = load_gtfs_file(i,"agency")
		gtfs.agency_head,gtfs.agency = load_gtfs_file(i,"agency")
		gtfs.agency_head,gtfs.agency = load_gtfs_file(i,"agency")
		gtfs.agency_head,gtfs.agency = load_gtfs_file(i,"agency")
		gtfs.agency_head,gtfs.agency = load_gtfs_file(i,"agency")

		fmt.Println(head)
		fmt.Println(data)

		merge_head(head,head1)
	}*/
	fmt.Println("end")
}

func load_gtfs_file(index int,filename string) (head map[string]int,records [][]string) {

	head = map[string]int{}
	records = [][]string{}

	file, err := os.Open("./"+filename)
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
/*        if file.IsDir() {
            paths = append(paths, dirwalk(filepath.Join(dir, file.Name()))...)
            continue
        }*/
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