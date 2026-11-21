# للبناء Rust استخدم أحدث صورة رسمية للغة
FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

# صورة التشغيل الخفيفة والصافية للإنتاج
FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/sovereign_engine /app/sovereign_engine

# فتح المنفذ الخاص بالخادم[cite: 15]
EXPOSE 8080

# تشغيل العقدة السيادية[cite: 15]
CMD ["./sovereign_engine"]
