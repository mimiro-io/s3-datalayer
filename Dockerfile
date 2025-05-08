FROM golang:1.22-alpine AS build

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY layer.go ./
COPY cmd ./cmd

# Build the app binaries
RUN CGO_ENABLED=0 GOOS=linux go build -a -o /s3-datalayer ./cmd/main.go

FROM gcr.io/distroless/static-debian12:nonroot
# Copy the Pre-built binary file from the previous stage
COPY --from=build /s3-datalayer /

# Expose port 8090 to the outside world
EXPOSE 8090

# Command to run the executable
CMD ["/s3-datalayer"]