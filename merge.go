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
		"github.com/google/uuid"
)

//go get "github.com/google/uuid"

var wg_unzip,wg_load sync.WaitGroup
var num_of_gtfs int

type gtfs_csv struct{
	head map[string]int
	data [][]string
}

func merge_head(head1 map[string]int,head2 map[string]int)(head map[string]int){
	head = map[string]int{}
	i := 0
	for str,_ := range head1 {
		if _, ok := head[str]; !ok{
			head[str] = i
			i++	
		}
	}
	for str,_ := range head2 {
		if _, ok := head[str]; !ok{
			head[str] = i
			i++	
		}
	}
	return
}

type gtfs_type map[string]gtfs_csv

func main() {
	uuidObj2, _ := uuid.NewUUID()
	fmt.Println(uuidObj2.String())
//	return;
	// initi
	num_of_gtfs = 0

	// unzip all gtfs
	paths,_ := dirwalk("./GTFS")
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
			csv_files,file_names := dirwalk("./unzip/"+strconv.Itoa(i)+"/")
			files := gtfs_type{}

			for i,file_name := range csv_files{
				var csv_file gtfs_csv
				csv_file.head,csv_file.data = load_gtfs_file(i,file_name)
				files[file_names[i]] = csv_file
			}
			gtfss[i]=files
		}(i)
	}
	wg_load.Wait()

	// change ids
	for i:=0;i<num_of_gtfs;i++ {
		// change stop_id
		new_stop_id := map[string]string{}
		new_trip_id := map[string]string{}
		new_route_id := map[string]string{}
		new_service_id := map[string]string{}
//		new_shape_id := map[string]string{}

		stops := gtfss[i]["stops.txt"]
		trips := gtfss[i]["trips.txt"]
		routes := gtfss[i]["routes.txt"]
		stop_times := gtfss[i]["stop_times.txt"]
		calendar := gtfss[i]["calendar.txt"]
		calendar_dates := gtfss[i]["calendar_dates.txt"]

		for i,_ := range stops.data {
			uuidObj, _ := uuid.NewUUID()
			uuidstr := uuidObj.String()
			new_stop_id[stops.data[i][stops.head["stop_id"]]] = uuidstr
			stops.data[i][stops.head["stop_id"]] = uuidstr
		}
		for i,_ := range routes.data {
			uuidObj, _ := uuid.NewUUID()
			uuidstr := uuidObj.String()
			new_route_id[routes.data[i][routes.head["route_id"]]] = uuidstr
			routes.data[i][routes.head["route_id"]] = uuidstr
		}
		for i,_ := range calendar.data {
			uuidObj, _ := uuid.NewUUID()
			uuidstr := uuidObj.String()
			new_service_id[calendar.data[i][calendar.head["service_id"]]] = uuidstr
			calendar.data[i][calendar.head["service_id"]] = uuidstr
		}
		for i,_ := range calendar_dates.data {
			calendar_dates.data[i][calendar_dates.head["service_id"]] = new_service_id[calendar_dates.data[i][calendar_dates.head["service_id"]]]
		}
		for i,_ := range trips.data {
			uuidObj, _ := uuid.NewUUID()
			uuidstr := uuidObj.String()
			new_trip_id[trips.data[i][trips.head["trip_id"]]] = uuidstr
			trips.data[i][trips.head["trip_id"]] = uuidstr
			trips.data[i][trips.head["service_id"]] = new_route_id[trips.data[i][trips.head["service_id"]]]
		}
		for i,_ := range stop_times.data {
			stop_times.data[i][stop_times.head["trip_id"]] = new_trip_id[stop_times.data[i][stop_times.head["trip_id"]]]
			stop_times.data[i][stop_times.head["stop_id"]] = new_stop_id[stop_times.data[i][stop_times.head["stop_id"]]]
		}

		gtfss[i]["stops.txt"] = stops
		gtfss[i]["trips.txt"] = trips
		gtfss[i]["routes.txt"] = routes
		gtfss[i]["stop_times.txt"] = stop_times
		gtfss[i]["calendar.txt"] = calendar
		gtfss[i]["calendar_dates.txt"] = calendar_dates
//		fmt.Println(gtfss[i]["stops.txt"])
	}

//	fmt.Println(gtfss)

	// Merge GTFS
//	gtfs_merged_data

	if err := os.Mkdir("./out/", 0777); err != nil {
		fmt.Println(err)
	}
	file_list := []string{"stops.txt","stop_times.txt","calendar.txt","calendar_dates.txt","trips.txt","routes.txt","agency.txt"}
	var wg sync.WaitGroup
	for _,str := range file_list{
		wg.Add(1)
		go func(str string){
			defer wg.Done()
			output_file(gtfss,str,num_of_gtfs)
		}(str)
	}
	wg.Wait()

	fmt.Println("end")
}

func output_file(gtfss map[int]gtfs_type,file_name string,num_of_gtfs int){
	out_file, err := os.Create("./out/"+file_name)
	if err != nil {
			// Openエラー処理
	}
	defer out_file.Close()
	var output []byte

	merged_head := map[string]int{} 
	for i:=0;i<num_of_gtfs;i++ {
		merged_head = merge_head(merged_head,gtfss[i][file_name].head)
	}
	fmt.Println(merged_head)

	isf2 := true
	var write_head []string
	for head,_ := range merged_head{
		if !isf2 {
			output = append(output,',')
		}
		isf2=false
		output = append(output,[]byte(head)...)
		write_head = append(write_head,head)
	}
	output = append(output,'\n')

	for i:=0;i<num_of_gtfs;i++ {
		stops := gtfss[i][file_name]
		for j,_ := range stops.data{
			isf := true
			for _,head := range write_head{
				if !isf {
					output = append(output,',')
				}
				isf=false
				output = append(output,[]byte(stops.data[j][stops.head[head]])...)
			}
			output = append(output,'\n')
		}
	}
	out_file.Write(([]byte)(string(output)))
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

func dirwalk(dir string) ([]string,[]string) {
    files, err := ioutil.ReadDir(dir)
    if err != nil {
        panic(err)
    }

    var paths,file_names []string
    for _, file := range files {
/*        if file.IsDir() {
            paths = append(paths, dirwalk(filepath.Join(dir, file.Name()))...)
            continue
        }*/
				paths = append(paths, filepath.Join(dir, file.Name()))
				file_names = append(file_names,file.Name())
    }

    return paths,file_names
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