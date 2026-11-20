<template>
  <section class="page" data-module="sink_dispatch">
    <header class="page-head">
      <div>
        <h2>下沉分发管理</h2>
        <p class="page-desc">维护湖表数据向OLAP、KV或搜索引擎下沉的分发规则，为下沉分发提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记分发规则</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出下沉分发清单</button>
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
            <button class="link" type="button" :data-testid="`dispatch-${row.id}`" @click="runDispatch(row, false)">执行分发</button>
            <button class="link" type="button" :data-testid="`dispatch-partial-${row.id}`" @click="runDispatch(row, true)">模拟部分失败</button>
            <button class="link" type="button" :data-testid="`dispatch-retry-${row.id}`" @click="runRetry(row)">重试续推</button>
            <button class="link" type="button" :data-testid="`receipts-${row.id}`" @click="toggleReceipts(row)">回执详情</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无下沉分发数据，可先登记分发规则</td>
        </tr>
      </tbody>
    </table>

    <section v-if="receiptRule" class="receipt-panel" data-testid="receipt-panel">
      <header class="receipt-head">
        <h3>分发幂等数据回执 — {{ receiptRule['规则编号'] }}（{{ receiptRule['湖表名'] }} → {{ receiptRule['目标系统'] }}）</h3>
        <button class="btn ghost" type="button" data-testid="close-receipts" @click="receiptRule = null">收起</button>
      </header>
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">目标系统聚合值</span>
          <strong class="stat-value" data-testid="ledger-aggregate">{{ ledger.aggregate_total ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已提交键数</span>
          <strong class="stat-value" data-testid="ledger-keys">{{ ledger.committed_keys ?? 0 }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">重复提交数</span>
          <strong class="stat-value" data-testid="ledger-duplicates">{{ ledger.duplicate_commits ?? 0 }}</strong>
        </article>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>回执编号</th>
            <th>批次键</th>
            <th>视图刷新版本</th>
            <th>已提交位置</th>
            <th>键总数</th>
            <th>回执状态</th>
            <th>数据版本</th>
            <th>重试次数</th>
            <th>提交确认</th>
            <th>收口路线</th>
            <th>更新时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="receipt in receipts" :key="String(receipt.receipt_id)" :data-testid="`receipt-${receipt.receipt_id}`">
            <td>{{ receipt.receipt_id }}</td>
            <td>{{ receipt.batch_key }}</td>
            <td>{{ receipt.view_refresh_version }}</td>
            <td :data-testid="`receipt-position-${receipt.receipt_id}`">{{ receipt.committed_position }}</td>
            <td>{{ receipt.total_keys }}</td>
            <td>{{ receipt.status }}</td>
            <td>{{ receipt.version }}</td>
            <td>{{ receipt.attempts }}</td>
            <td>{{ receipt.confirmation ? `已确认#${receipt.confirmation.confirm_seq}` : '—' }}</td>
            <td>{{ receipt.resolution ?? '—' }}</td>
            <td>{{ receipt.updated_at ?? '—' }}</td>
          </tr>
          <tr v-if="!receipts.length">
            <td colspan="11" class="empty-state">暂无回执：执行分发后按批次键生成幂等回执</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记分发规则</h3>
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
      <span>共 {{ total }} 条下沉分发记录</span>
      <span v-if="noticeMessage" :class="noticeLevel === 'warn' ? 'warn-text' : 'notice-text'" data-testid="notice">{{ noticeMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type Receipt = Record<string, any>

const ENDPOINT = '/api/sink_dispatch'
const columns = ["规则编号", "湖表名", "目标系统", "写入模式", "字段映射", "过滤条件", "批次大小", "失败回退"]
const actions = ["提交校验", "标记生效", "停用规则"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["分发规则总数", "待校验规则", "已生效规则"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const noticeMessage = ref('')
const noticeLevel = ref<'ok' | 'warn'>('ok')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const receiptRule = ref<Row | null>(null)
const receipts = ref<Receipt[]>([])
const ledger = ref<Record<string, any>>({})
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
    link.download = 'sink_dispatch.csv'
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
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('下沉分发动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下沉分发操作失败'
  }
}

async function runDispatch(row: Row, simulatePartialFailure: boolean) {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/dispatch`, {
      method: 'POST',
      body: JSON.stringify({ simulate_partial_failure: simulatePartialFailure }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '分发执行失败')
    noticeLevel.value = payload.entry?.interrupted ? 'warn' : 'ok'
    noticeMessage.value = payload.message
    await reload()
    await openReceipts(row)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发执行失败'
  }
}

async function runRetry(row: Row) {
  errorMessage.value = ''
  noticeMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/retry`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '重试续推失败')
    noticeLevel.value = 'ok'
    noticeMessage.value = payload.message
    await reload()
    await openReceipts(row)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重试续推失败'
  }
}

async function toggleReceipts(row: Row) {
  if (receiptRule.value?.id === row.id) {
    receiptRule.value = null
    return
  }
  await openReceipts(row)
}

async function openReceipts(row: Row) {
  receiptRule.value = row
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/receipts`)
    if (!response.ok) throw new Error('分发幂等数据回执读取失败')
    const payload = await response.json()
    receipts.value = payload.receipts ?? []
    ledger.value = payload.ledger ?? {}
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分发幂等数据回执读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('分发规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    if (receiptRule.value) {
      await openReceipts(receiptRule.value)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下沉分发列表读取失败'
  }
}

onMounted(reload)
</script>
