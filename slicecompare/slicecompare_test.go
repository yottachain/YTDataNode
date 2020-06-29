package slicecompare

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func Testvalue( ) {
	jsonStr := []byte(`{"age":1}`)
	var value map[string]interface{}
	json.Unmarshal(jsonStr, &value)
	age := value["age"]
	fmt.Println(age)
	fmt.Println(reflect.TypeOf(age))
}

/*
func init() {
	var hashkeymap = map[string] string{"0001":"e32109f56abcde678","0002":"e32109f56abcde986"}
}
*/

func TestSlicecompare(t *testing.T) {
	/*
	fmt.Println("++++before clean DB++++")
	getAllReordFromDB(FileDB_sn)
	getAllReordFromDB(FileDB_todel)
	getAllReordFromDB(FileDB_tmp)



	fmt.Println("++++after clean DB++++")
	getAllReordFromDB(FileDB_sn)
	getAllReordFromDB(FileDB_todel)
	getAllReordFromDB(FileDB_tmp)

	return
	*/
	slcp := NewSliceComparer()
	slcp.GetAllReordFromDB(FileDB_sn)
	slcp.GetAllReordFromDB(FileDB_todel)
	slcp.GetAllReordFromDB(FileDB_tmp)
	/*
	slcp.cleanDB(FileDB_sn)
	slcp.cleanDB(FileDB_todel)
	slcp.cleanDB(FileDB_tmp)

	hashbatch:= [][]byte{
		[]byte("e32109f56abcde678"),
		[]byte("e32109f56abcde986"),
		[]byte("e32109f56aabc6899"),
	}
	hashbatch2:= [][]byte{
		[]byte("111111f56abcde678"),
		[]byte("222222f56abcde986"),
		[]byte("333333f56aabc6899"),
		[]byte("441111f56abcde670"),
		[]byte("552222f56abcde981"),
		[]byte("663333f56aabc6892"),
		[]byte("711111f56abcde673"),
		[]byte("822222f56abcde984"),
		[]byte("933333f56aabc6895"),
		[]byte("a11111f56abcde676"),
		[]byte("b22222f56abcde987"),
		[]byte("c33333f56aabc6898"),
	}

	hashbatch3:= [][]byte{
		[]byte("101111f56abcde678"),
		[]byte("202222f56abcde986"),
		[]byte("303333f56aabc6899"),
		[]byte("401111f56abcde670"),
		[]byte("502222f56abcde981"),
		[]byte("603333f56aabc6892"),
	}

//	var hashkey = map[string] string{"0001":"e32109f56abcde678","0002":"e32109f56abcde986"}
	fmt.Println(" ==== FileDB_tmp =====  ")
	slcp.SaveRecordToTmpDB(hashbatch, FileDB_tmp)
	time.Sleep(2*time.Second)
	slcp.SaveRecordToTmpDB(hashbatch2, FileDB_tmp)
	time.Sleep(2*time.Second)
	slcp.SaveRecordToTmpDB(hashbatch3, FileDB_tmp)
	slcp.GetAllReordFromDB(FileDB_tmp)


	fmt.Println(" ==== FileDB_sn =====  ")
//	saveRecordToTmpDB(hashbatch, FileDB_tmp)
//	saveSnRecordToDB(hashbatch, "1000", FileDB_sn, FileFirstEntryIdx)
	slcp.SaveSnRecordToDB(hashbatch2, "2000", FileDB_sn, FileFirstEntryIdx)
	slcp.GetAllReordFromDB(FileDB_sn)

	fmt.Println(" ==== after compared  FileDB_tmp =====  ")
	slcp.CompareEntryWithSnTables(hashbatch2, FileDB_tmp, FileComparedIdx)
	slcp.GetAllReordFromDB(FileDB_tmp)
//	Testvalue()
	indexForDld,_ := slcp.GetValueFromFile(FileComparedIdx)
	fmt.Printf("index=%s\n",indexForDld)

/*
	fmt.Println(" ==== after compared  FileDB_tmp ===== ")
	saveSnRecordToDB(hashbatch2, "2000", FileDB_sn, FileFirstEntryIdx)
	getAllReordFromDB(FileDB_sn)
	indexForDld = getIndexForDownloadFromSN()
	fmt.Printf("index=%d\n",indexForDld)

	saveSnRecordToDB(hashbatch2, "3000", FileDB_sn, FileFirstEntryIdx)
	getAllReordFromDB(FileDB_sn)
	indexForDld = getIndexForDownloadFromSN()
	fmt.Printf("index=%d\n",indexForDld)
*/

	fmt.Println(" ==== get entry from  FileDB_todel ===== ")
//	slcp.SaveEntryInDBToDel(FileDB_tmp,FileDB_todel)
	slcp.GetAllReordFromDB(FileDB_todel)

//	indexForDld,_ = slcp.GetValueFromFile(FileComparedIdx)
//	fmt.Printf("index=%s\n",indexForDld)

}


