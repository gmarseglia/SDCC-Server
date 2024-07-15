package utils

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	pb "server/proto"
)

const Step = 25

func SimulatedCPUIntensiveFunction(baseDurationMillis float64, active *int, multiplier int) {

	var counter float64

	for counter < baseDurationMillis {
		counter += Step / (float64(*active) * float64(multiplier))
		time.Sleep(Step * time.Millisecond)
	}

}

func DummyCPUIntensiveFunction(iterations int) float64 {
	result := 0.0

	for i := 0; i < iterations; i++ {
		result += math.Sqrt(float64(i))
		result *= math.Pow(math.Sin(float64(i)), 2)
		result /= math.Pow(math.Cos(float64(i)), 2)
	}

	return result
}

// Get preferred outbound ip of this machine
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func SetupFieldOptional(field *string, envName string, defaultValue string) {
	setupField(false, field, envName, defaultValue, nil)
}

func SetupFieldMandatory(field *string, envName string, callback func()) {
	setupField(true, field, envName, "", callback)
}

func setupField(mandatory bool, field *string, envName string, defaultValue string, callback func()) {
	if *field == "" {
		*field = os.Getenv(envName)
		if *field == "" {
			if mandatory {
				callback()
				return
			} else {
				*field = defaultValue
			}
		}
	}
}

func SetupFieldInt(mandatory bool, field *int, envName string, defaultValue int, callback func()) {
	if *field == -1 {
		envRes := os.Getenv(envName)
		num, err := strconv.Atoi(envRes)
		if err != nil {
			if mandatory {
				callback()
				return
			} else {
				*field = defaultValue
			}
		} else {
			*field = num
		}
	}
}

func SetupFieldBool(field *bool, envName string) {
	if !*field {
		_, found := os.LookupEnv(envName)
		*field = found
	}
}

func PrettyPrint(title string, matrix [][]float32) {
	// Print the matrix with pretty formatting
	fmt.Println(title)
	for _, row := range matrix {
		for _, value := range row {
			fmt.Printf("%.4f ", value)
		}
		fmt.Println()
	}
}

func MatrixToProto(matrix [][]float32) *pb.Matrix {
	protoMatrix := &pb.Matrix{}
	for _, row := range matrix {
		protoRow := &pb.Row{}
		for _, value := range row {
			protoRow.Values = append(protoRow.Values, value)
		}
		protoMatrix.Rows = append(protoMatrix.Rows, protoRow)
	}
	return protoMatrix
}

func ProtoToMatrix(protoMatrix *pb.Matrix) [][]float32 {
	matrix := [][]float32{}
	for _, protoRow := range protoMatrix.Rows {
		row := []float32{}
		for _, value := range protoRow.Values {
			row = append(row, value)
		}
		matrix = append(matrix, row)
	}
	return matrix
}

func GenerateEmptyMatrix(height int, width int) [][]float32 {
	result := make([][]float32, height)
	for i := 0; i < height; i++ {
		result[i] = make([]float32, width)
	}
	return result
}

func GenerateRandomMatrix(height int, width int) [][]float32 {
	result := make([][]float32, height)
	for i := 0; i < height; i++ {
		result[i] = make([]float32, width)
		for j := 0; j < width; j++ {
			// result[i][j] = float32(rand.Float32()*2 - 1)
			result[i][j] = float32(1)
		}
	}
	return result
}

func GenerateMatrix(height int, width int, random bool, value float32) [][]float32 {
	result := make([][]float32, height)
	for i := 0; i < height; i++ {
		result[i] = make([]float32, width)
		for j := 0; j < width; j++ {
			if random {
				result[i][j] = float32(rand.Float32()*2 - 1)
			} else {
				result[i][j] = value
			}
		}
	}
	return result
}
