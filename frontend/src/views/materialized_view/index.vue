<template>
  <section class="page" data-module="materialized_view">
    <header class="page-head">
      <div>
        <h2>物化查询管理</h2>
        <p class="page-desc">维护湖上物化视图的查询定义、增量刷新与存储配置，为物化查询提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记物化视图</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出物化查询清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" :data-testid="`verify-${row.id}`" @click="verifyVersion(row)">核对刷新版本</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无物化查询数据，可先登记物化视图</td>
        </tr>
      </tbody>
    </table>

    <section v-if="verifyRow && verifyResult" class="receipt-panel" data-testid="verify-panel">
      <header class="receipt-head">
        <h3>刷新版本核对 — {{ verifyRow['视图编号'] }}（基表 {{ verifyResult['基表名'] }}）</h3>
        <button class="btn ghost" type="button" data-testid="close-verify" @click="closeVerify">收起</button>
      </header>
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">当前刷新版本</span>
          <strong class="stat-value" data-testid="current-refresh-version">{{ verifyResult['当前刷新版本'] }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">目标系统聚合值</span>
          <strong class="stat-value" data-testid="verify-aggregate">{{ verifyResult.ledger?.aggregate_total ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已提交键数</span>
          <strong class="stat-value">{{ verifyResult.ledger?.committed_keys ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">重复提交数</span>
          <strong class="stat-value" data-testid="verify-duplicates">{{ verifyResult.ledger?.duplicate_commits ?? 0 }}</strong>
        </article>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>回执编号</th>
            <th>批次键</th>
            <th>分发规则</th>
            <th>目标系统</th>
            <th>回执刷新版本</th>
            <th>已提交位置</th>
            <th>回执状态</th>
            <th>提交确认</th>
            <th>版本核对</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in verifyResult.receipts" :key="String(item.receipt_id)" :data-testid="`verify-receipt-${item.receipt_id}`">
            <td>{{ item.receipt_id }}</td>
            <td>{{ item.batch_key }}</td>
            <td>{{ item['规则编号'] }}</td>
            <td>{{ item.target_system }}</td>
            <td>{{ item.receipt_refresh_version }}</td>
            <td>{{ item.committed_position }}/{{ item.total_keys }}</td>
            <td>{{ item.status }}</td>
            <td>{{ item.confirmation ? `已确认#${item.confirmation.confirm_seq}` : '—' }}</td>
            <td :data-testid="`verify-result-${item.receipt_id}`">{{ item['核对结果'] }}</td>
          </tr>
          <tr v-if="!verifyResult.receipts?.length">
            <td colspan="9" class="empty-state">暂无分发回执：下沉分发执行后此处核对视图刷新版本</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记物化视图</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条物化查询记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type VerifyResult = Record<string, any>

const ENDPOINT = '/api/materialized_view'
const columns = ["视图编号", "查询SQL", "基表名", "刷新模式", "刷新间隔", "存储位置", "数据量", "最后刷新"]
const actions = ["提交校验", "启用刷新", "停用视图"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const metricNames = ["物化视图总数", "待校验视图", "已启用视图"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const verifyRow = ref<Row | null>(null)
const verifyResult = ref<VerifyResult | null>(null)
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'materialized_view.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('物化查询动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '物化查询操作失败'
  }
}

async function verifyVersion(row: Row) {
  errorMessage.value = ''
  if (verifyRow.value?.id === row.id) {
    closeVerify()
    return
  }
  try {
    const response = await request(`${ENDPOINT}/${row.id}/dispatch_receipts`)
    if (!response.ok) throw new Error('刷新版本核对失败，请稍后重试')
    verifyResult.value = await response.json()
    verifyRow.value = row
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '刷新版本核对失败'
  }
}

function closeVerify() {
  verifyRow.value = null
  verifyResult.value = null
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('物化视图列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    if (verifyRow.value) {
      const current = verifyRow.value
      verifyRow.value = null
      await verifyVersion(current)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '物化视图列表读取失败'
  }
}

onMounted(reload)
</script>
