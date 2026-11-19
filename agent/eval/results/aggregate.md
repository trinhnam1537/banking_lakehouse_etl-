# Tổng hợp nhiều lượt đánh giá agent

Agent ontology: 1 lượt (20261119-221605). Agent đối chứng: 1 lượt (20261119-221605-baseline). Sinh bởi `python -m agent.eval.aggregate`.

## 1. Độ chính xác theo lượt

| Agent | Lượt | Trên mọi câu chấm được | Trên câu chung |
|---|---|---:|---:|
| ontology | 20261119-221605 | 91.7% | 90.0% |
| đối chứng | 20261119-221605-baseline | 68.0% | 68.0% |

| Agent | Trung bình (câu chung) | CI 95% theo lượt |
|---|---:|---|
| ontology | 90.0% | cần >= 2 lượt |
| đối chứng | 68.0% | cần >= 2 lượt |

## 2. So sánh ghép cặp trên cùng câu

50 câu chung. Ontology 90.0% vs đối chứng 68.0%: chênh lệch **+22.0%** (bootstrap theo câu, CI 95% [+8.0%; +38.0%]). McNemar trên kết quả đa số: chỉ ontology đạt 14 câu, chỉ đối chứng đạt 3 câu, p = 0.0153.

## 3. Theo nhóm câu và use case

| Nhóm | Số câu | Ontology | Đối chứng |
|---|---:|---:|---:|
| phai tu choi | 12 | 92% | 50% |
| thong ke | 24 | 88% | 75% |
| tra cuu | 14 | 93% | 71% |
| use case A | 21 | 95% | 76% |
| use case B | 10 | 100% | 60% |
| use case C | 12 | 75% | 67% |
| use case E | 7 | 86% | 57% |

## 4. Câu không ổn định giữa các lượt (agent ontology)

không có

Câu trượt ở MỌI lượt (agent ontology): A12, C04, C09, C10, E04
