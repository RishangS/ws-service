# Set the name of the Go executable
APP_NAME = ws-service

# Set the Go source directory (if applicable)
SRC_DIR = .

# Default target
all: build

# Build the Go application
build:
	@echo "Building the application..."
	go build -o bin/$(APP_NAME) $(SRC_DIR)

# Run the Go application
run: build
	@echo "Running the application..."
	./bin/$(APP_NAME)

# Clean the build
clean:
	@echo "Cleaning the build..."
	rm -f bin/$(APP_NAME)

# Install Go dependencies
deps:
	@echo "Installing dependencies..."
	go mod tidy

# Format the Go code
fmt:
	@echo "Formatting the code..."
	go fmt ./...

# Lint the Go code
lint:
	@echo "Linting the code..."
	golangci-lint run
