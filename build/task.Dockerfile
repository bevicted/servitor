# syntax=docker/dockerfile:1
# This Dockerfile expects a build context with sibling servitor-oss and ict
# source directories. `make task-image` creates that minimal context.
ARG GO_VERSION=1.24.0
ARG TERRAFORM_VERSION=1.5.7

FROM golang:${GO_VERSION}-bookworm AS servitor-build
WORKDIR /src/servitor
COPY servitor-oss/go.mod servitor-oss/go.sum ./
RUN go mod download
COPY servitor-oss/api ./api
COPY servitor-oss/cmd ./cmd
COPY servitor-oss/internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/servitor-task ./cmd/servitor-task

FROM golang:1.23.0-bookworm AS ict-build
WORKDIR /src/ict
COPY ict/go.mod ict/go.sum ./
RUN go mod download
COPY ict ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/ict .

FROM hashicorp/terraform:${TERRAFORM_VERSION} AS terraform

FROM gcr.io/distroless/base-debian12:nonroot
LABEL org.opencontainers.image.title="servitor-task" \
      org.opencontainers.image.description="Servitor ICT and Terraform task runtime" \
      org.opencontainers.image.version="terraform-1.5.7-ibm-provider-2.5.0"
COPY --from=servitor-build /out/servitor-task /usr/local/bin/servitor-task
COPY --from=ict-build /out/ict /usr/local/bin/ict
# ICT embeds this lock at build time. Keeping it in the image also makes the
# provider version used for plan, apply, and destroy directly inspectable.
COPY --from=ict-build /src/ict/internal/terraform/assets/.terraform.lock.hcl /usr/share/ict/.terraform.lock.hcl
COPY --from=terraform /bin/terraform /usr/local/bin/terraform
USER 65532:0
ENTRYPOINT ["/usr/local/bin/servitor-task"]
