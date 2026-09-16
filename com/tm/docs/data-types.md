# Bốn loại dữ liệu: MUTEX · NOT_MUTEX · PARTIAL_VALUE · PARTIAL_VALUE_BY_TAG

> Giải thích trực quan cho `CLAUDE.md` §3.2. Định nghĩa hình thức và công thức ở `CLAUDE.md` §3–§4.
> Mọi ví dụ tính tại **15/09**: A1 = 15/09 · A7 = 09/09→15/09 · A30 = 17/08→15/09.

Cả bốn loại cùng trả lời một câu hỏi: **user X có thuộc tag T trong khoảng ngày W không?**
Khác nhau ở **dữ liệu đưa vào** và **cách gom dữ liệu trong khoảng ngày**.

| Dữ liệu vào | Loại |
|---|---|
| **Nhãn**: "gắn tag này" (ADD) / "gỡ tag này" (REMOVE) | MUTEX, NOT_MUTEX |
| **Con số**: vd số tiền giao dịch (kèm tag nhóm nếu BY_TAG) | PARTIAL_VALUE, PARTIAL_VALUE_BY_TAG |

---

## 1. MUTEX — mỗi lúc chỉ một giá trị

Tag loại trừ nhau: tại một thời điểm user có **tối đa 1 tag**. Gắn tag mới = thay tag cũ.
Ví dụ: `churn_score_band` (low/mid/high), `user_city`, `age_band`.

**User An:**
```
01/09  ADD high
09/09  ADD low
12/09  ADD mid
```

**Luật:** trong khoảng ngày, lấy **lần ADD gần nhất**; nếu tag đó bị REMOVE sau lần ADD → không thuộc tag nào.

| Khoảng ngày | ADD trong khoảng | ADD gần nhất | An thuộc |
|---|---|---|---|
| A1 | không có | — | không tag nào |
| A7 | low (09), mid (12) | mid | **mid** |
| A30 | high, low, mid | mid | **mid** |
| Custom 01→10 | high (01), low (09) | low | **low** |

- Không lấy "gần nhất" thì trong A7 An thuộc cả `low` và `mid` → vô lý.
- Thêm `13/09 REMOVE mid` → A7: **không thuộc tag nào**. **Không quay lại `low`**.

---

## 2. NOT_MUTEX — nhiều tag cùng lúc, mỗi tag độc lập

Như một checklist: gắn/gỡ tag này không ảnh hưởng tag khác.
Ví dụ: `product_holding` (paylater/insurance/savings), `txn_category` (fnb/travel/bill).

**User Bình:**
```
03/09  ADD paylater
10/09  ADD insurance
12/09  REMOVE paylater
14/09  ADD savings
```

**Luật:** xét **từng tag riêng** — trong khoảng ngày, tín hiệu gần nhất của tag đó là ADD → có tag.

| Khoảng ngày | paylater | insurance | savings | Bình thuộc |
|---|---|---|---|---|
| A7 | REMOVE (12) ❌ | ADD (10) ✅ | ADD (14) ✅ | **insurance, savings** |
| A30 | REMOVE cuối ❌ | ✅ | ✅ | **insurance, savings** |
| Custom 01→05 | ADD (03) ✅ | — | — | **paylater** |

**Khai báo sai loại → segment sai.** Cùng chuỗi trên nếu coi là MUTEX: A7 lấy ADD gần nhất = savings → Bình chỉ có **savings**, mất insurance dù vẫn đang có bảo hiểm.
Câu hỏi quyết định: *"Một người có thể có 2 giá trị cùng lúc không?"*

---

## Chú ý chung cho MUTEX/NOT_MUTEX: EVENT hay STATE

Bảng của Bình ở **A1 cho kết quả rỗng** (15/09 không có tín hiệu), dù Bình vẫn đang giữ insurance, savings.
Luật "chỉ nhìn tín hiệu trong khoảng" hợp với **sự kiện** ("7 ngày qua có mua F&B?") nhưng không hợp với **trạng thái** ("đang ở đâu", "đang có sản phẩm gì").

| feedMode | Dùng cho | Kết quả |
|---|---|---|
| `EVENT` | sự kiện đã xảy ra | tính như các bảng trên |
| `STATE` | trạng thái đang có | trạng thái hiện tại coi như được ADD lại mỗi ngày → mọi khoảng ngày = trạng thái hiện tại (Bình: insurance, savings) |

---

## 3. PARTIAL_VALUE — con số, cộng dồn rồi so khoảng giá trị

Nguồn gửi **con số**. Hệ thống **cộng các con số trong khoảng ngày**, rồi xem tổng nằm trong `valueRange` nào.
Ví dụ `txn_amount`, tag = khoảng giá trị: `lt_500k` [0, 500K) · `500k_2m` [500K, 2M) · `gte_2m` [2M, ∞).

**User Chi:**
```
01/09  1.500.000
10/09    300.000
15/09    200.000
15/09    100.000
```

| Khoảng ngày | Khoản trong khoảng | Tổng | Chi thuộc |
|---|---|---|---|
| A1 | 200K + 100K | 300.000 | **lt_500k** |
| A7 | 300K + 200K + 100K | 600.000 | **500k_2m** |
| A30 | cả 4 | 2.100.000 | **gte_2m** |
| Custom 01→09 | 1.5M | 1.500.000 | **500k_2m** |

- Cùng user, **mỗi khoảng ngày có thể ra tag khác** (tổng thay đổi theo độ dài).
- Segment có thể **tự đặt ngưỡng** (ad-hoc `valueRange`): "A7 ≥ 1M" → ❌, "A30 ≥ 1M" → ✅.
- **Không có giao dịch trong khoảng → không thuộc range nào, kể cả `lt_500k`.** Muốn "không giao dịch 7 ngày" → dùng `SUB`.

## 4. PARTIAL_VALUE_BY_TAG — con số tách theo nhóm

Mỗi con số kèm một tag; hệ thống **cộng riêng từng tag**. Condition bắt buộc có `valueRange`.

**User Chi** (có phân loại):
```
01/09  travel  1.500.000
10/09  fnb       300.000
15/09  fnb       200.000
15/09  bill      100.000
```

| Condition | Tổng | Kết quả |
|---|---|---|
| fnb, A7, ≥ 500K | 500K | ✅ |
| bill, A7, ≥ 500K | 100K | ❌ |
| travel, A7, ≥ 1M | không có giao dịch | ❌ |
| travel, A30, ≥ 1M | 1.5M | ✅ |

---

## Tóm tắt

| | MUTEX | NOT_MUTEX | PARTIAL_VALUE | PARTIAL_VALUE_BY_TAG |
|---|---|---|---|---|
| Dữ liệu vào | ADD/REMOVE tag | ADD/REMOVE tag | con số | con số + tag nhóm |
| Trong khoảng ngày | ADD gần nhất của cả attribute | từng tag: tín hiệu gần nhất là ADD | SUM → so `valueRange` | SUM từng tag → so `valueRange` |
| Số tag / user | ≤ 1 | nhiều | ≤ 1 nếu range không chồng nhau | mỗi tag một tổng riêng |
| Ngưỡng | — | — | tag định sẵn hoặc ad-hoc | luôn ad-hoc trong condition |
| Ví dụ | city, churn band, age band | sản phẩm đang dùng, loại giao dịch | tổng chi tiêu | chi theo ngành hàng |
| Lưu trữ | bitmap | bitmap | row `(uidx, sum)` | row `(tag, uidx, sum)` |

## Chọn loại khi tạo attribute mới

1. Dữ liệu là **con số cần cộng dồn**? → `PARTIAL_VALUE` (cần tách theo nhóm → `PARTIAL_VALUE_BY_TAG`).
2. Là nhãn: **một người có thể có 2 giá trị cùng lúc?** Không → `MUTEX` · Có → `NOT_MUTEX`.
3. Nhãn là **trạng thái đang có** hay **sự kiện đã xảy ra**? → `STATE` / `EVENT`.
