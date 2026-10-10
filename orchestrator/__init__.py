"""Orchestrator: MỘT cửa vào cho mọi luồng job (sourcing, silver, sau này gold).

    orchestrator/trigger.py    Lambda trigger — nhận job_id, kích Step Functions
    orchestrator/job_types.py  mỗi luồng một JobType: đọc config ở đâu, kiểm
                               tra thế nào, truyền tham số Glue ra sao, ai là
                               luồng kế tiếp
    orchestrator/events/       event mẫu để test trên Lambda console

Hạ tầng: terraform/modules/orchestrator.
"""
