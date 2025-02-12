package messages

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
)

const MAX_REPLICAS = 3
const HEARTBEAT_PER_SEC = 5

// ValidateFile /* validate file */
func ValidateFile(fileCheckSum string, newFileName string) (bool, string) {
	isValid := false
	file, err := os.Open(newFileName)
	if err != nil {
		log.Fatalln(err.Error())
		os.Exit(1)
	}

	//reset file pointer
	file.Seek(0, 0)

	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}

	newFileCheckSum := hex.EncodeToString(h.Sum(nil))

	if newFileCheckSum == fileCheckSum {
		isValid = true
	} else {
		isValid = false
	}
	//fmt.Println("Done validating file")
	return isValid, newFileCheckSum
}

// HandleError log error and print it
func HandleError(e error) {
	if e != nil {
		log.Fatalln(e)
		panic(e)
	}
}
