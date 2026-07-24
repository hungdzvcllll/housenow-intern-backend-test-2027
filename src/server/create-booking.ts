import type { Selectable } from "kysely";
import type {
  AppDatabase,
  BookingTable,
} from "../db";
import { jsonError } from "../errors";

interface CreateBookingBody {
  userId?: unknown;
  slotId?: unknown;
  idempotencyKey?: unknown;
}

type Booking = Selectable<BookingTable>;

function serializeBooking(row: Booking) {
  return {
    id: row.id,
    userId: row.user_id,
    slotId: row.slot_id,
    status: row.status,
  };
}

async function readBody(request: Request): Promise<CreateBookingBody | null> {
  try {
    return (await request.json()) as CreateBookingBody;
  } catch {
    return null;
  }
}

export async function handleCreateBookingRequest(
  request: Request,
  db: AppDatabase,
): Promise<Response> {
  const body = await readBody(request);
  const { userId, slotId, idempotencyKey } = body ?? {};

  // 1. Validate request
  if (
    !Number.isInteger(userId) ||
    !Number.isInteger(slotId) ||
    typeof idempotencyKey !== "string" ||
    idempotencyKey.trim().length === 0
  ) {
    return jsonError(
      400,
      "VALIDATION_ERROR",
      "userId, slotId and idempotencyKey are required",
    );
  }

  // 2. Kiểm tra Idempotency trước (xử lý Retry/Duplicate Request)
  const existingBooking = await db
    .selectFrom("bookings")
    .selectAll()
    .where("idempotency_key", "=", idempotencyKey)
    .executeTakeFirst();

  if (existingBooking) {
    // Trả lại kết quả booking cũ nếu request bị trùng/retry
    return Response.json(serializeBooking(existingBooking), { status: 200 });
  }

  try {
    // 3. Chạy Transaction để đảm bảo tính nhất quán & chống Race Condition
    const booking = await db.transaction().execute(async (trx) => {
      // Check User tồn tại
      const user = await trx
        .selectFrom("users")
        .select("id")
        .where("id", "=", userId as number)
        .executeTakeFirst();

      if (!user) {
        throw { status: 404, code: "USER_NOT_FOUND", message: "User was not found" };
      }

      // Lock dòng Slot lại (Pessimistic Lock) để chống 2 request cùng sửa số chỗ đồng thời
      const slot = await trx
        .selectFrom("slots")
        .select(["id", "remaining"])
        .where("id", "=", slotId as number)
        .forUpdate() // <--- KHÓA DÒNG NÀY LẠI CHO ĐẾN KHI TRANSACTION XONG
        .executeTakeFirst();

      if (!slot) {
        throw { status: 404, code: "SLOT_NOT_FOUND", message: "Slot was not found" };
      }

      if (slot.remaining <= 0) {
        throw { status: 409, code: "SLOT_FULL", message: "Slot is fully booked" };
      }

      // Trừ chỗ
      await trx
        .updateTable("slots")
        .set(({ eb }) => ({
          remaining: eb("remaining", "-", 1),
        }))
        .where("id", "=", slotId as number)
        .execute();

      // Tạo booking
      return await trx
        .insertInto("bookings")
        .values({
          user_id: userId as number,
          slot_id: slotId as number,
          idempotency_key: idempotencyKey,
        })
        .returningAll()
        .executeTakeFirstOrThrow();
    });

    return Response.json(serializeBooking(booking), { status: 201 });
  } catch (error: any) {
    // Nếu là lỗi nghiệp vụ do ta tự throw ở trên
    if (error?.status && error?.code) {
      return jsonError(error.status, error.code, error.message);
    }

    return jsonError(500, "INTERNAL_ERROR", "Unexpected booking error");
  }
}
