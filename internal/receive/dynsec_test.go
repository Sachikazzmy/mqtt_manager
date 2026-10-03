package receive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"Project/internal/device"
)

type scriptedTransport struct {
	calls    [][]dynsecCommand
	answers  [][]dynsecResponse
	callErrs []error
}

func (t *scriptedTransport) Execute(_ context.Context, commands []dynsecCommand) ([]dynsecResponse, error) {
	copyOfCommands := append([]dynsecCommand(nil), commands...)
	t.calls = append(t.calls, copyOfCommands)
	index := len(t.calls) - 1
	if index < len(t.callErrs) && t.callErrs[index] != nil {
		return nil, t.callErrs[index]
	}
	if index >= len(t.answers) {
		return nil, fmt.Errorf("unexpected management call %d", index)
	}
	return t.answers[index], nil
}

func success(command string) dynsecResponse {
	return dynsecResponse{Command: command}
}

func notFound(command string) dynsecResponse {
	return dynsecResponse{Command: command, Error: "Role not found"}
}

func clientData(id, role string, disabled bool) json.RawMessage {
	value, _ := json.Marshal(map[string]any{
		"client": map[string]any{
			"username": id,
			"disabled": disabled,
			"roles":    []map[string]any{{"rolename": role, "priority": 10}},
		},
	})
	return value
}

func deviceRoleData(id string) json.RawMessage {
	return roleData(deviceRoleName(id), legacyDeviceACLs(id))
}

func roleData(name string, acls []dynsecACL) json.RawMessage {
	value, _ := json.Marshal(map[string]any{
		"role": map[string]any{
			"rolename": name,
			"acls":     acls,
		},
	})
	return value
}

func TestUpgradeExistingDeviceACLPreservesClientAndCompletesPartialUpgrade(t *testing.T) {
	id := "linux-01"
	oldClient := clientData(id, deviceRoleName(id), false)
	fullRole := roleData(deviceRoleName(id), deviceACLs(id))
	first := &scriptedTransport{answers: [][]dynsecResponse{
		{{Command: "getClient", Data: oldClient}},
		{{Command: "getRole", Data: deviceRoleData(id)}},
		{success("addRoleACL")},
		{{Command: "addRoleACL", Error: "temporary failure"}},
	}}
	manager := &Manager{transport: first}
	if err := manager.UpgradeDeviceACL(context.Background(), id); err == nil {
		t.Fatal("故意中断的部分 ACL 升级应报告失败")
	}
	if len(first.calls) != 4 || first.calls[2][0].Command != "addRoleACL" || first.calls[3][0].ACLType != "subscribeLiteral" {
		t.Fatalf("部分升级调用序列 = %#v", first.calls)
	}
	for _, calls := range first.calls {
		for _, call := range calls {
			if call.Command == "setClientPassword" || call.Command == "disableClient" || call.Command == "createClient" {
				t.Fatalf("ACL 升级不应重建或轮换设备账户: %#v", call)
			}
		}
	}

	partial := append(append([]dynsecACL(nil), legacyDeviceACLs(id)...), deviceACLs(id)[1])
	second := &scriptedTransport{answers: [][]dynsecResponse{
		{{Command: "getClient", Data: oldClient}},
		{{Command: "getRole", Data: roleData(deviceRoleName(id), partial)}},
		{success("addRoleACL")},
		{success("addRoleACL")},
		{{Command: "getRole", Data: fullRole}},
	}}
	manager.transport = second
	if err := manager.UpgradeDeviceACL(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if len(second.calls) != 5 || second.calls[0][0].Username != id || second.calls[2][0].ACLType != "subscribeLiteral" || second.calls[3][0].ACLType != "publishClientReceive" {
		t.Fatalf("恢复后的 ACL 升级调用 = %#v", second.calls)
	}
}

func TestBootstrapUpgradesBackendRoleWithoutResettingExistingAccount(t *testing.T) {
	config := Config{ReceiverUsername: "__project01_receiver__", ReceiverPassword: []byte("configured-secret")}
	oldRole := roleData(receiverRoleName(), legacyReceiverACLs())
	fullRole := roleData(receiverRoleName(), receiverACLs())
	transport := &scriptedTransport{answers: [][]dynsecResponse{
		{success("setDefaultACLAccess")},
		{{Command: "getRole", Data: oldRole}},
		{success("addRoleACL")},
		{success("addRoleACL")},
		{success("addRoleACL")},
		{{Command: "getRole", Data: fullRole}},
		{{Command: "getClient", Data: clientData(config.ReceiverUsername, receiverRoleName(), false)}},
	}}
	manager := NewManager(config)
	manager.transport = transport
	if err := manager.BootstrapReceiver(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, calls := range transport.calls {
		for _, call := range calls {
			if call.Command == "setClientPassword" || call.Command == "disableClient" || call.Command == "enableClient" || call.Command == "createClient" {
				t.Fatalf("升级已有后端角色不应改动账户或密钥: %#v", call)
			}
		}
	}
}

func TestCreateDeviceCreatesNarrowRoleAndAccount(t *testing.T) {
	transport := &scriptedTransport{answers: [][]dynsecResponse{
		{notFound("getRole")},
		{success("createRole")},
		{notFound("getClient")},
		{success("createClient")},
	}}
	manager := &Manager{transport: transport}
	if err := manager.CreateDevice(context.Background(), "device-001", "one-time-secret"); err != nil {
		t.Fatal(err)
	}
	if len(transport.calls) != 4 {
		t.Fatalf("管理调用次数=%d", len(transport.calls))
	}
	roleCommand := transport.calls[1][0]
	if roleCommand.Command != "createRole" || roleCommand.RoleName != deviceRoleName("device-001") || len(roleCommand.ACLs) != 4 {
		t.Fatalf("设备角色命令 = %#v", roleCommand)
	}
	wantACLs := deviceACLs("device-001")
	for index, acl := range roleCommand.ACLs {
		if acl != wantACLs[index] {
			t.Fatalf("设备 ACL[%d] = %#v, want %#v", index, acl, wantACLs[index])
		}
	}
	clientCommand := transport.calls[3][0]
	if clientCommand.Username != "device-001" || clientCommand.Password != "one-time-secret" || len(clientCommand.Roles) != 1 || clientCommand.Roles[0].Name != roleCommand.RoleName {
		t.Fatalf("设备账户命令 = %#v", clientCommand)
	}
}

func TestBootstrapReceiverSetsDenyDefaultsAndSeparateReadRole(t *testing.T) {
	config := Config{ReceiverUsername: "__project01_receiver__", ReceiverPassword: []byte("receiver-secret")}
	transport := &scriptedTransport{answers: [][]dynsecResponse{
		{success("setDefaultACLAccess")},
		{notFound("getRole")},
		{success("createRole")},
		{notFound("getClient")},
		{success("createClient")},
	}}
	manager := NewManager(config)
	manager.transport = transport
	if err := manager.BootstrapReceiver(context.Background()); err != nil {
		t.Fatal(err)
	}
	defaults := transport.calls[0][0]
	if len(defaults.ACLs) != 4 {
		t.Fatalf("默认 ACL 数量=%d", len(defaults.ACLs))
	}
	for _, acl := range defaults.ACLs {
		if acl.Allow {
			t.Fatalf("默认 ACL 应全部拒绝: %#v", acl)
		}
	}
	role := transport.calls[2][0]
	if role.RoleName != receiverRoleName() || len(role.ACLs) != 5 {
		t.Fatalf("订阅角色命令 = %#v", role)
	}
	for _, acl := range role.ACLs {
		if !acl.Allow || (acl.Topic != telemetrySubscription && acl.Topic != resultSubscription && acl.Topic != "factory/+/command") {
			t.Fatalf("后端角色 ACL 范围错误: %#v", acl)
		}
	}
	client := transport.calls[4][0]
	if client.Username != config.ReceiverUsername || client.Password != string(config.ReceiverPassword) || client.Username == "device-001" {
		t.Fatalf("后端订阅账户没有与设备账户分离: %#v", client)
	}
}

func TestResetDisablesOldSessionBeforePasswordAndRestoresRequestedState(t *testing.T) {
	id := "device-001"
	role := deviceRoleName(id)
	transport := &scriptedTransport{answers: [][]dynsecResponse{
		{{Command: "getClient", Data: clientData(id, role, false)}},
		{{Command: "getRole", Data: deviceRoleData(id)}},
		{success("disableClient"), success("setClientPassword"), success("enableClient")},
	}}
	manager := &Manager{transport: transport}
	if err := manager.ResetDeviceSecret(context.Background(), id, "new-secret", true); err != nil {
		t.Fatal(err)
	}
	commands := transport.calls[2]
	if len(commands) != 3 || commands[0].Command != "disableClient" || commands[1].Command != "setClientPassword" || commands[2].Command != "enableClient" {
		t.Fatalf("在线账户密钥重置顺序 = %#v", commands)
	}
	if commands[1].Password != "new-secret" {
		t.Fatal("新密钥未传给密钥重置命令")
	}
}

func TestResetOfAlreadyDisabledAccountIsRetryable(t *testing.T) {
	id := "device-001"
	transport := &scriptedTransport{answers: [][]dynsecResponse{
		{{Command: "getClient", Data: clientData(id, deviceRoleName(id), true)}},
		{{Command: "getRole", Data: deviceRoleData(id)}},
		{success("setClientPassword")},
	}}
	manager := &Manager{transport: transport}
	if err := manager.ResetDeviceSecret(context.Background(), id, "next-secret", false); err != nil {
		t.Fatal(err)
	}
	commands := transport.calls[2]
	if len(commands) != 1 || commands[0].Command != "setClientPassword" {
		t.Fatalf("禁用状态重试不应依赖再次 disable: %#v", commands)
	}
}

func TestDeleteRevokesConnectedAccountBeforeDeletingIt(t *testing.T) {
	id := "device-001"
	transport := &scriptedTransport{answers: [][]dynsecResponse{
		{{Command: "getClient", Data: clientData(id, deviceRoleName(id), false)}},
		{{Command: "getRole", Data: deviceRoleData(id)}},
		{success("disableClient"), success("deleteClient")},
	}}
	manager := &Manager{transport: transport}
	if err := manager.DeleteDevice(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	commands := transport.calls[2]
	if len(commands) != 2 || commands[0].Command != "disableClient" || commands[1].Command != "deleteClient" {
		t.Fatalf("删除账户撤权顺序 = %#v", commands)
	}
}

func TestManagerPreservesUncertainResultsAndRejectsForeignAccounts(t *testing.T) {
	transportErr := errors.New("lost management response")
	manager := &Manager{transport: &scriptedTransport{callErrs: []error{transportErr}}}
	if err := manager.CreateDevice(context.Background(), "device-001", "secret"); !errors.Is(err, transportErr) {
		t.Fatalf("管理接口传输错误未保留: %v", err)
	}

	id := "device-001"
	foreign := clientData(id, "other-role", false)
	manager = &Manager{transport: &scriptedTransport{answers: [][]dynsecResponse{{{Command: "getClient", Data: foreign}}}}}
	if err := manager.DeleteDevice(context.Background(), id); err == nil || errors.Is(err, device.ErrBrokerStateUncertain) {
		t.Fatalf("应拒绝操作角色不匹配的账户且状态确定，错误=%v", err)
	}
}

func TestValidateDeviceClientRejectsGroupMembership(t *testing.T) {
	id := "device-001"
	role := deviceRoleName(id)
	data, err := json.Marshal(map[string]any{
		"client": map[string]any{
			"username": id,
			"roles":    []map[string]any{{"rolename": role, "priority": 10}},
			"groups":   []map[string]any{{"groupname": "extra-permissions", "priority": 10}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDeviceClient(data, id, role); err == nil {
		t.Fatal("账户加入额外组后应拒绝，组可能授予其他 Topic 权限")
	}
}

func TestSetDeviceEnabledRejectsBroadenedRoleACL(t *testing.T) {
	id := "device-001"
	broadenedRole, err := json.Marshal(map[string]any{
		"role": map[string]any{
			"rolename": deviceRoleName(id),
			"acls": []map[string]any{
				{"acltype": "publishClientSend", "topic": telemetryTopic(id), "priority": 10, "allow": true},
				{"acltype": "publishClientSend", "topic": "factory/+/telemetry", "priority": 10, "allow": true},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	transport := &scriptedTransport{answers: [][]dynsecResponse{
		{{Command: "getClient", Data: clientData(id, deviceRoleName(id), true)}},
		{{Command: "getRole", Data: broadenedRole}},
	}}
	manager := &Manager{transport: transport}
	if err := manager.SetDeviceEnabled(context.Background(), id, true); err == nil {
		t.Fatal("设备角色增加跨设备 ACL 后应拒绝启用")
	}
	if len(transport.calls) != 2 {
		t.Fatalf("拒绝扩权角色后不应发送启用命令: calls=%d", len(transport.calls))
	}
}

func TestValidateDeviceRoleRequiresExactTopic(t *testing.T) {
	data := json.RawMessage(fmt.Sprintf(`{"role":{"rolename":%q,"acls":[{"acltype":"publishClientSend","topic":"factory/+/telemetry","priority":10,"allow":true}]}}`, deviceRoleName("device-001")))
	if err := validateDeviceRole(data, deviceRoleName("device-001"), "factory/device-001/telemetry"); err == nil {
		t.Fatal("带通配符的设备角色应拒绝")
	}
}

func TestEnabledDynamicSecurityClientMayOmitDisabledField(t *testing.T) {
	data := json.RawMessage(`{"client":{"username":"device-001","roles":[]}}`)
	disabled, err := clientDisabled(data)
	if err != nil {
		t.Fatalf("缺省 disabled 应表示启用状态: %v", err)
	}
	if disabled {
		t.Fatal("缺省 disabled 被解析为禁用")
	}
}
