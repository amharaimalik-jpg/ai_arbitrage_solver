# استخدم أحدث صورة رسمية للغة Rust للبناء
FROM rust:latest AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

# صورة التشغيل الخفيفة والصافية للإنتاج
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/sovereign_engine /app/sovereign_engine

# فتح المنفذ الخاص بالخادم
EXPOSE 8080

# تشغيل العقدة السيادية
CMD ["./sovereign_engine"]