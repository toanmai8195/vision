# Loại dữ liệu — theo nguồn hiện có: MUTEX + STATE

> Giải thích trực quan cho `CLAUDE.md` §3.2. Định nghĩa hình thức và công thức ở `CLAUDE.md` §3–§4.
> Ví dụ tính tại **15/09/2026** (dữ liệu golden `data-flow-examples.md` §2.1): A1 = 15/09 · A7 = 09/09→15/09 · A30 = 17/08→15/09.
>
> **Phạm vi hiện tại: chỉ nguồn `user_profile` → loại `MUTEX` + `STATE`.** Tài liệu chỉ giải thích những gì nguồn này dùng tới. Khi thêm nguồn ở bước 11–16 (`checklist.md`) thì bổ sung loại tương ứng vào đây (bảng ở cuối). Định nghĩa đầy đủ cả 4 loại vẫn ở `CLAUDE.md` §3.2; bản giải thích đầy đủ cũ (đủ 4 loại, `aggFunc`, `EXTENDED`) có trong git, commit `f07750a`.

Mọi loại cùng trả lời một câu hỏi: **user X có thuộc tag T trong khoảng ngày W không?**
Khác nhau ở **dữ liệu đưa vào** và **cách gom dữ liệu trong khoảng ngày**. Nguồn hiện tại đưa vào **nhãn** ("user ở thành phố X", "giới tính Y") nên chỉ dùng nhóm nhãn.

---

## 1. Nguồn hiện có: `user_profile`

Bảng `src.user_profile`, 1 dòng/user, có sửa/xoá, đi qua CDC (`problem.md` §1.1):

| Cột | Ý nghĩa | Dùng làm |
|---|---|---|
| `city_code` | thành phố hiện tại (HCM, HN…); NULL = chưa có/đã xoá | attribute `user_city` |
| `gender` | M / F / O; NULL = chưa khai báo | ứng viên `gender` |
| `birth_date` | ngày sinh | ứng viên `age_band` (nhãn tuổi tính ở silver) |

Dữ liệu ví dụ:

| User | Diễn biến |
|---|---|
| U1001 (uidx 1) | HCM từ 10/01/2025, **đổi sang HN** ngày 15/09 |
| U1002 (uidx 2) | user mới, vào ngày 15/09 với HCM |
| U1003 (uidx 3) | HN từ 2024, **không đổi** |

Cả ba thuộc nhóm **"một người chỉ có một giá trị tại một thời điểm"** → `MUTEX`. Và đây là **trạng thái đang có** (không phải sự kiện) → `STATE`.

---

## 2. MUTEX — mỗi lúc chỉ một giá trị

Tag loại trừ nhau: tại một thời điểm user có **tối đa 1 tag**. Gắn tag mới = thay tag cũ.
Ví dụ ở nguồn này: `user_city` (hcm / hn / …), `gender` (m / f / o), `age_band`.

**Luật:** trong khoảng ngày, lấy **lần ADD gần nhất**; nếu tag đó bị REMOVE sau lần ADD → không thuộc tag nào (và **không quay lại tag cũ**).

Với `user_city`, thay đổi ở OLTP được hiểu như sau:

| Thay đổi ở OLTP | Với `user_city` |
|---|---|
| user mới / đổi city sang giá trị X | ADD tag X (tag cũ tự bị thay) |
| `city_code` thành NULL, hoặc xoá dòng | REMOVE → không thuộc tag nào |

**U1001** đổi city: ADD `hcm` (từ 2025) → ADD `hn` (15/09).

| Khoảng ngày | ADD trong khoảng | ADD gần nhất | U1001 thuộc |
|---|---|---|---|
| A1 | hn (15/09) | hn | **hn** |
| A7 | hn (15/09) | hn | **hn** |
| Custom 01/09 → 10/09 | không có ADD mới (hcm từ 2025) | — | xem mục STATE bên dưới |

Không lấy "gần nhất" thì U1001 thuộc cả `hcm` và `hn` → vô lý.

**Câu hỏi quyết định chọn MUTEX:** *"Một người có thể có 2 giá trị cùng lúc không?"* — Một người chỉ ở một thành phố, một giới tính → không → `MUTEX`.

---

## 3. STATE — trạng thái đang có (khác EVENT)

`EVENT` chỉ nhìn **tín hiệu xảy ra trong khoảng ngày**. Hợp với sự kiện ("7 ngày qua có mua F&B?") nhưng **sai với trạng thái** ("đang ở đâu").

**Thử coi `user_city` là EVENT** (chỉ có tín hiệu khi city đổi):

| Khoảng ngày | U1001 | U1002 | **U1003** |
|---|---|---|---|
| A7 | hn | hcm | **không thuộc tag nào** |
| A30 | hn | hcm | **không thuộc tag nào** |

U1003 ở HN từ 2024, không có thay đổi nào trong 30 ngày → không có tín hiệu → mất khỏi `hn`. Sai: U1003 vẫn đang ở HN.

**`STATE`** sửa điều này: trạng thái hiện tại **được coi là ADD lại mỗi ngày**, nên mọi khoảng ngày kết thúc ở `ds` đều bằng trạng thái hiện tại; mất giá trị = REMOVE.

| Khoảng ngày | hcm | hn |
|---|---|---|
| A1, A7, A30, …, ALWAYS_ACTIVE | {U1002} | {U1001, U1003} |

(Cùng kết quả `{2}` và `{1,3}` ở `data-flow-examples.md` §2.1.)

| feedMode | Dùng cho | Kết quả |
|---|---|---|
| `EVENT` | sự kiện đã xảy ra | chỉ tín hiệu trong khoảng ngày |
| `STATE` | trạng thái đang có (city, giới tính, tuổi) | mọi khoảng ngày = trạng thái hiện tại |

Window **kết thúc trước hôm nay** (custom range lịch sử, vd 01/09→10/09) lấy trạng thái tại ngày kết thúc: ở 10/09 U1001 còn ở `hcm`, U1002 chưa tồn tại, U1003 ở `hn` → hcm = {U1001}, hn = {U1003}.

---

## 4. Ứng viên attribute từ `user_profile`

| Attribute | Loại | Tag | Ghi chú |
|---|---|---|---|
| `user_city` | MUTEX + STATE | hcm, hn, dn, ct… | **attribute đầu tiên** (golden `seg_0001`) |
| `gender` | MUTEX + STATE | m, f, o | NULL = chưa khai báo → không thuộc tag nào (U1003 ở ví dụ) |
| `age_band` | MUTEX + STATE | vd 18–24, 25–34, 35–44, 45+ (**đề xuất**, chốt ở bước 5) | derived: tuổi = `datediff(ds, birth_date)` tính ở silver; Debezium phát `birth_date` là số ngày từ 1970 |

`age_band` ví dụ tại 15/09/2026: U1001 (sinh 20/05/1990, 36 tuổi) → 35–44; U1002 (sinh 03/11/2001, 24 tuổi) → 18–24; U1003 (sinh 14/02/1985, 41 tuổi) → 35–44. Ngày 03/11/2026 U1002 tròn 25 tuổi: tag `18–24` REMOVED, `25–34` ADDED mà **không có thay đổi nào ở OLTP** — vì band tính theo `ds` ở silver.

---

## 5. Chọn loại khi tạo attribute mới (phần áp dụng hiện tại)

1. Dữ liệu là **nhãn** (không phải con số cần cộng dồn)? → nhóm `MUTEX` / `NOT_MUTEX`.
2. **Một người có thể có 2 giá trị cùng lúc không?** Không → `MUTEX`. (Có → `NOT_MUTEX`, chưa có nguồn nào dùng.)
3. Nhãn là **trạng thái đang có** hay **sự kiện đã xảy ra**? Trạng thái → `STATE`; sự kiện → `EVENT`.
4. Giá trị suy ra từ cột khác (tuổi từ ngày sinh, band từ số) → tính ở silver như derived attribute, vào như `STATE`.

## 6. Sẽ bổ sung khi có nguồn

| Khi thêm | Bổ sung vào tài liệu này |
|---|---|
| Bước 11 — payment, `MUTEX` `EVENT` (`last_txn_category`) | MUTEX với tín hiệu sự kiện, `REMOVE` |
| Bước 12 — `NOT_MUTEX` `EVENT` (`txn_category`) | nhiều tag cùng lúc, tín hiệu gần nhất của từng tag; khai báo sai loại → segment sai |
| Bước 13 — `PARTIAL_VALUE`, `PARTIAL_VALUE_BY_TAG`, `aggFunc` | con số, cộng dồn, `valueRange`, SUM/COUNT/MIN/MAX |
| Bước 14 — product, `NOT_MUTEX` `STATE` | NOT_MUTEX với STATE |
| Bước 15 — voucher/OA/app, `EXTENDED` | tag chuỗi tự do, tính theo nhu cầu (usage-driven) |
| Bước 16 — churn file, `MUTEX` `EVENT` + REMOVE | band theo điểm, REMOVE, không quay lại tag cũ |
| Cuối | bảng tóm tắt so sánh 4 loại, cây chọn loại đầy đủ |
