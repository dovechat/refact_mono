import uuid
import jwt
import pytest

from core.security import ALGORITHMS, SECRET_KEY


def _tenant_headers(user_id, tenant_id, member: str) -> dict:
    token = jwt.encode(
        {"sub": str(user_id), "type": "access", "tenant_id": str(tenant_id), "member": member},
        SECRET_KEY,
        algorithm=ALGORITHMS,
    )
    return {"Authorization": f"Bearer {token}"}


class TestTenantRBAC:

    async def test_get_tenant_wrong_tenant_403(self, client, token_with_tenant, test_tenant):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.get(
            f"/api/v1/tenant/{foreign_tenant_id}",
            headers=token_with_tenant,
        )

        assert resp.status_code == 403
        assert resp.json()["detail"] == "Token tenant_id mismatch"

    async def test_get_tenant_own_tenant_200(self, client, token_with_tenant, test_tenant):
        resp = await client.get(
            f"/api/v1/tenant/{test_tenant.id}",
            headers=token_with_tenant,
        )

        assert resp.status_code == 200
        assert resp.json()["id"] == str(test_tenant.id)

    async def test_update_tenant_wrong_tenant_403(self, client, token_with_tenant):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.patch(
            f"/api/v1/tenant/{foreign_tenant_id}",
            json={"name": "hacked"},
            headers=token_with_tenant,
        )

        assert resp.status_code == 403

    async def test_delete_tenant_wrong_tenant_403(self, client, token_with_tenant):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.delete(
            f"/api/v1/tenant/{foreign_tenant_id}",
            headers=token_with_tenant,
        )

        assert resp.status_code == 403

    async def test_update_banking_wrong_tenant_403(self, client, token_with_tenant):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.patch(
            f"/api/v1/tenant/{foreign_tenant_id}/banking/ru",
            json={},
            headers=token_with_tenant,
        )

        assert resp.status_code == 403

    async def test_crm_settings_get_wrong_tenant_403(self, client, token_with_tenant):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.get(
            f"/api/v1/tenant/{foreign_tenant_id}/crm-settings",
            headers=token_with_tenant,
        )

        assert resp.status_code == 403
        assert resp.json()["detail"] == "Token tenant_id mismatch"


class TestLineRBAC:

    async def test_list_lines_wrong_tenant_403(self, client, token_with_tenant):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.get(
            f"/api/v1/tenants/{foreign_tenant_id}/lines/",
            headers=token_with_tenant,
        )

        assert resp.status_code == 403

    async def test_list_lines_own_tenant_200(self, client, token_with_tenant, test_tenant):
        resp = await client.get(
            f"/api/v1/tenants/{test_tenant.id}/lines/",
            headers=token_with_tenant,
        )

        assert resp.status_code == 200

    async def test_get_line_wrong_tenant_403(self, client, token_with_tenant, test_line):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.get(
            f"/api/v1/tenants/{foreign_tenant_id}/lines/{test_line.id}",
            headers=token_with_tenant,
        )

        assert resp.status_code == 403

    async def test_create_line_wrong_tenant_403(self, client, token_with_tenant):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.post(
            f"/api/v1/tenants/{foreign_tenant_id}/lines/",
            json={"name": "hacked line"},
            headers=token_with_tenant,
        )

        assert resp.status_code == 403

    async def test_delete_line_wrong_tenant_403(self, client, token_with_tenant, test_line):
        foreign_tenant_id = uuid.uuid4()

        resp = await client.delete(
            f"/api/v1/tenants/{foreign_tenant_id}/lines/{test_line.id}",
            headers=token_with_tenant,
        )

        assert resp.status_code == 403


class TestRoleRBAC:

    async def test_member_cannot_adjust_balance_403(self, client, test_user, test_tenant):
        resp = await client.post(
            f"/api/v1/tenants/{test_tenant.id}/billing/balance/adjust",
            json={"amount": 10, "balance_type": "main", "description": "x"},
            headers=_tenant_headers(test_user.id, test_tenant.id, "member"),
        )
        assert resp.status_code == 403

    async def test_owner_cannot_adjust_balance_403(self, client, test_user, test_tenant):
        resp = await client.post(
            f"/api/v1/tenants/{test_tenant.id}/billing/balance/adjust",
            json={"amount": 10, "balance_type": "main", "description": "x"},
            headers=_tenant_headers(test_user.id, test_tenant.id, "owner"),
        )
        assert resp.status_code == 403

    async def test_member_cannot_update_tenant_403(self, client, test_user, test_tenant):
        resp = await client.patch(
            f"/api/v1/tenant/{test_tenant.id}",
            json={"name": "hacked"},
            headers=_tenant_headers(test_user.id, test_tenant.id, "member"),
        )
        assert resp.status_code == 403

    async def test_admin_member_cannot_delete_tenant_403(self, client, test_user, test_tenant):
        resp = await client.delete(
            f"/api/v1/tenant/{test_tenant.id}",
            headers=_tenant_headers(test_user.id, test_tenant.id, "admin"),
        )
        assert resp.status_code == 403

    async def test_member_cannot_add_member_403(self, client, test_user, test_tenant):
        resp = await client.post(
            f"/api/v1/tenants/{test_tenant.id}/members",
            json={"email": "new@example.com", "role": "member"},
            headers=_tenant_headers(test_user.id, test_tenant.id, "member"),
        )
        assert resp.status_code == 403

    async def test_member_can_read_tenant_200(self, client, test_user, test_tenant):
        resp = await client.get(
            f"/api/v1/tenant/{test_tenant.id}",
            headers=_tenant_headers(test_user.id, test_tenant.id, "member"),
        )
        assert resp.status_code == 200
