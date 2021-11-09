package hdf5utils

import (
	"fmt"
	"os"

	"github.com/google/uuid"
)

var PipeRoot = "/workspaces/pipes"

func ClosePipes(pipes []string) {
	for _, p := range pipes {
		os.Remove(p)
	}
}

func GetPipe() (string, error) {
	uuid, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s", PipeRoot, uuid.String()), err
}
