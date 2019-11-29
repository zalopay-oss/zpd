############################
# STEP 1 build executable binary
############################
FROM golang:alpine as builder
RUN apk update && apk add git
# Create appuser.
# RUN adduser -D -g '' appuser

# WORKDIR /myapp

# COPY go.mod go.sum ./
# RUN go mod download

# COPY . /myapp

# RUN rm -rf ~/.netrc

# # build server
# ARG BUILD_CONTEXT=${BUILD_CONTEXT:-"/myapp/cmd/server"}

# ENV BUILD_CONTEXT=${BUILD_CONTEXT}

# WORKDIR $BUILD_CONTEXT

# # Build the binary.
# RUN GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GO111MODULE=on go build -ldflags="-w -s" -o /go/bin/myapp

############################
# STEP 2 build a small image
############################
# FROM scratch
# # Import the user and group files from the builder.
# COPY --from=builder /etc/passwd /etc/passwd
# ARG BUILD_CONTEXT

# WORKDIR /myapp

# # Copy our static executable.
# COPY --from=builder /go/bin/myapp ./
# # COPY --from=builder /myapp/cmd/server/config.yaml ./
# COPY --from=builder /myapp/cmd/server/logFile.log ./

# Use an unprivileged user.
# USER appuser

# EXPOSE 10000

# Run the hello binary.
# ENTRYPOINT ["./myapp"]

# CMD ping localhost
    