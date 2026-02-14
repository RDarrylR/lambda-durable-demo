import { useState, useEffect, useRef } from 'react'

const API_URL = import.meta.env.VITE_API_URL || ''

const DEMO_PROFILES = [
  {
    label: 'Alice — Always Approved',
    name: 'Alice Johnson',
    address: '123 Maple Street, Ottawa, ON K1A 0B1',
    phone: '613-555-0101',
    sin: '1111',
    loan_amount: 150000,
  },
  {
    label: 'Bob — Always Denied',
    name: 'Bob Martinez',
    address: '456 Oak Avenue, Toronto, ON M5V 2T6',
    phone: '416-555-0202',
    sin: '2222',
    loan_amount: 30000,
  },
  {
    label: 'Charlie — Limited to $25K',
    name: 'Charlie Wilson',
    address: '789 Pine Road, Vancouver, BC V6B 3K9',
    phone: '604-555-0303',
    sin: '3333',
    loan_amount: 20000,
  },
]

const WORKFLOW_STEPS = [
  { key: 'submitted', label: 'Submitted' },
  { key: 'validating', label: 'Validating' },
  { key: 'credit_check', label: 'Credit Check' },
  { key: 'risk_assessment', label: 'Risk Assessment' },
  { key: 'manager_approval', label: 'Manager Approval' },
  { key: 'fraud_check', label: 'Fraud Check' },
  { key: 'generating_offer', label: 'Generating Offer' },
  { key: 'disbursing', label: 'Disbursing' },
  { key: 'complete', label: 'Complete' },
]

const TERMINAL_STATUSES = new Set(['approved', 'denied', 'failed'])

function App() {
  const [form, setForm] = useState({
    name: '',
    address: '',
    phone: '',
    sin: '',
    loan_amount: '',
  })
  const [applicationId, setApplicationId] = useState(null)
  const [status, setStatus] = useState(null)
  const [submitting, setSubmitting] = useState(false)
  const [showLogs, setShowLogs] = useState(false)
  const [error, setError] = useState(null)
  const [approveSending, setApproveSending] = useState(false)
  const logEndRef = useRef(null)

  const isPolling = applicationId && status && !TERMINAL_STATUSES.has(status.status)

  // Poll for status updates
  useEffect(() => {
    if (!isPolling) return

    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${API_URL}/status/${applicationId}`)
        if (res.ok) {
          const data = await res.json()
          setStatus(data)
        }
      } catch {
        // Silently retry on network errors
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [applicationId, isPolling])

  // Auto-scroll debug logs
  useEffect(() => {
    if (showLogs && logEndRef.current) {
      logEndRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [status?.logs?.length, showLogs])

  function fillProfile(profile) {
    setForm({
      name: profile.name,
      address: profile.address,
      phone: profile.phone,
      sin: profile.sin,
      loan_amount: String(profile.loan_amount),
    })
    setApplicationId(null)
    setStatus(null)
    setError(null)
  }

  function resetForm() {
    setForm({ name: '', address: '', phone: '', sin: '', loan_amount: '' })
    setApplicationId(null)
    setStatus(null)
    setError(null)
  }

  async function handleSubmit(e) {
    e.preventDefault()
    setError(null)
    setSubmitting(true)

    try {
      const res = await fetch(`${API_URL}/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...form,
          loan_amount: parseFloat(form.loan_amount),
        }),
      })

      const data = await res.json()

      if (!res.ok) {
        setError(data.error || 'Submission failed')
        setSubmitting(false)
        return
      }

      setApplicationId(data.application_id)
      setStatus({
        application_id: data.application_id,
        status: 'submitted',
        current_step: 'submitted',
        logs: [{ timestamp: new Date().toISOString(), step: 'submitted', message: 'Application received', level: 'info' }],
      })
    } catch {
      setError('Network error — is the API running?')
    } finally {
      setSubmitting(false)
    }
  }

  async function handleApprove(approved) {
    setApproveSending(true)
    try {
      const res = await fetch(`${API_URL}/approve/${applicationId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ approved }),
      })
      if (!res.ok) {
        const data = await res.json()
        setError(data.error || 'Approval request failed')
      }
    } catch {
      setError('Network error sending approval')
    } finally {
      setApproveSending(false)
    }
  }

  const needsManagerApproval = status?.status === 'pending_approval'

  function getStepIndex(stepKey) {
    return WORKFLOW_STEPS.findIndex(s => s.key === stepKey)
  }

  const currentStepIndex = status ? getStepIndex(status.current_step) : -1

  return (
    <div className="app">
      <header>
        <h1>Lambda Durable Functions</h1>
        <p className="subtitle">Loan Approval Workflow Demo</p>
      </header>

      <main>
        {/* Demo Profile Buttons */}
        <section className="profiles">
          <h2>Demo Profiles</h2>
          <div className="profile-buttons">
            {DEMO_PROFILES.map((p) => (
              <button
                key={p.sin}
                className="profile-btn"
                onClick={() => fillProfile(p)}
                disabled={submitting || isPolling}
              >
                {p.label}
              </button>
            ))}
          </div>
        </section>

        {/* Application Form */}
        <section className="form-section">
          <h2>Loan Application</h2>
          <form onSubmit={handleSubmit}>
            <div className="form-grid">
              <label>
                Full Name
                <input
                  type="text"
                  value={form.name}
                  onChange={e => setForm({ ...form, name: e.target.value })}
                  required
                  disabled={submitting || isPolling}
                />
              </label>
              <label>
                Address
                <input
                  type="text"
                  value={form.address}
                  onChange={e => setForm({ ...form, address: e.target.value })}
                  disabled={submitting || isPolling}
                />
              </label>
              <label>
                Phone Number
                <input
                  type="text"
                  value={form.phone}
                  onChange={e => setForm({ ...form, phone: e.target.value })}
                  disabled={submitting || isPolling}
                />
              </label>
              <label>
                SIN (last 4 digits)
                <input
                  type="text"
                  value={form.sin}
                  onChange={e => setForm({ ...form, sin: e.target.value })}
                  maxLength={4}
                  pattern="\d{4}"
                  required
                  disabled={submitting || isPolling}
                />
              </label>
              <label>
                Loan Amount ($)
                <input
                  type="number"
                  value={form.loan_amount}
                  onChange={e => setForm({ ...form, loan_amount: e.target.value })}
                  min="1"
                  required
                  disabled={submitting || isPolling}
                />
              </label>
            </div>

            {error && <p className="error">{error}</p>}

            <div className="form-actions">
              <button type="submit" className="submit-btn" disabled={submitting || isPolling}>
                {submitting ? 'Submitting...' : 'Submit Application'}
              </button>
              {status && TERMINAL_STATUSES.has(status.status) && (
                <button type="button" className="reset-btn" onClick={resetForm}>
                  New Application
                </button>
              )}
            </div>
          </form>
        </section>

        {/* Status Display */}
        {status && (
          <section className="status-section">
            <h2>Application Status</h2>
            <p className="app-id">ID: {status.application_id}</p>

            {/* Step Indicator */}
            <div className="steps">
              {WORKFLOW_STEPS.map((step, i) => {
                let state = 'pending'
                if (i < currentStepIndex) state = 'completed'
                else if (i === currentStepIndex) {
                  state = TERMINAL_STATUSES.has(status.status) ? 'completed' : 'active'
                }

                return (
                  <div key={step.key} className={`step ${state}`}>
                    <div className="step-indicator">
                      {state === 'completed' ? (
                        <span className="check">&#10003;</span>
                      ) : state === 'active' ? (
                        <span className="spinner" />
                      ) : (
                        <span className="dot" />
                      )}
                    </div>
                    <span className="step-label">{step.label}</span>
                  </div>
                )
              })}
            </div>

            {/* Final Result */}
            {status.status === 'approved' && status.result && (
              <div className="result approved">
                <h3>APPROVED</h3>
                <div className="result-details">
                  <p><strong>Offer ID:</strong> {status.result.offer_id}</p>
                  <p><strong>Loan Amount:</strong> ${Number(status.result.loan_amount).toLocaleString()}</p>
                  <p><strong>Annual Rate:</strong> {status.result.annual_rate}%</p>
                  <p><strong>Monthly Payment:</strong> ${Number(status.result.monthly_payment).toLocaleString(undefined, { minimumFractionDigits: 2 })}</p>
                  <p><strong>Term:</strong> {status.result.term_months} months</p>
                  <p><strong>Disbursement Ref:</strong> {status.result.disbursement_ref}</p>
                </div>
              </div>
            )}

            {status.status === 'denied' && status.result && (
              <div className="result denied">
                <h3>DENIED</h3>
                <div className="result-details">
                  <p><strong>Reason:</strong> {status.result.reason}</p>
                  {status.result.risk_tier && <p><strong>Risk Tier:</strong> {status.result.risk_tier}</p>}
                  {status.result.average_score && <p><strong>Avg Credit Score:</strong> {status.result.average_score}</p>}
                </div>
              </div>
            )}

            {status.status === 'failed' && (
              <div className="result failed">
                <h3>FAILED</h3>
                <p>The workflow encountered an error.</p>
              </div>
            )}
          </section>
        )}

        {/* Manager Approval Modal */}
        {needsManagerApproval && (
          <div className="modal-overlay">
            <div className="modal">
              <h3>Manager Approval Required</h3>
              <p>
                This loan application for <strong>${Number(form.loan_amount).toLocaleString()}</strong> requires
                manager approval because the amount is $100,000 or more.
              </p>
              <p className="modal-subtext">The workflow is suspended and waiting for your decision.</p>
              <div className="modal-actions">
                <button
                  className="approve-btn"
                  onClick={() => handleApprove(true)}
                  disabled={approveSending}
                >
                  {approveSending ? 'Sending...' : 'Approve'}
                </button>
                <button
                  className="deny-btn"
                  onClick={() => handleApprove(false)}
                  disabled={approveSending}
                >
                  Deny
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Debug Log Panel */}
        {status && (
          <section className="logs-section">
            <div className="logs-header">
              <h2>Debug Logs</h2>
              <label className="toggle">
                <input
                  type="checkbox"
                  checked={showLogs}
                  onChange={e => setShowLogs(e.target.checked)}
                />
                <span>Show Logs</span>
              </label>
            </div>

            {showLogs && status.logs && (
              <div className="log-panel">
                {status.logs.map((log, i) => (
                  <div key={i} className={`log-entry log-${log.level}`}>
                    <span className="log-ts">{new Date(log.timestamp).toLocaleTimeString()}</span>
                    <span className="log-step">[{log.step}]</span>
                    <span className="log-msg">{log.message}</span>
                  </div>
                ))}
                <div ref={logEndRef} />
              </div>
            )}
          </section>
        )}
      </main>
    </div>
  )
}

export default App
