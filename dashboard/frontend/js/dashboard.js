const liveAlerts = document.getElementById("live-alerts");
const historyBody = document.getElementById("history-body");
const connectionStatus = document.getElementById("connection-status");
const connectionDot = document.getElementById("connection-dot");
const alertCount = document.getElementById("alert-count");
const notificationContainer = document.getElementById("notification-container");

let alertTotal = 0;
let authToken = localStorage.getItem("tofd_admin_token") || "";


/* =========================================================
   WEBSOCKET - LIVE ALERTS
   ========================================================= */

function connectWebSocket() {
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${location.host}/api/ws/alerts`);

    ws.onopen = () => {
        connectionStatus.textContent = "LIVE";
        connectionDot.style.background = "#3fb950";
    };

    ws.onmessage = (event) => {
        const alert = JSON.parse(event.data);

        // Live alerts only.
        // Do NOT add live alerts to history because history
        // must respect the active filters.
        addLiveAlert(alert);
        showNotification(alert);
        showSystemNotification(alert);
    };

    ws.onclose = () => {
        connectionStatus.textContent = "DISCONNECTED";
        connectionDot.style.background = "#f85149";

        setTimeout(connectWebSocket, 3000);
    };

    ws.onerror = () => {
        ws.close();
    };
}


function addLiveAlert(alert) {
    const empty = liveAlerts.querySelector(".empty-state");

    if (empty) {
        empty.remove();
    }

    alertTotal++;
    alertCount.textContent =
        `${alertTotal} alert${alertTotal === 1 ? "" : "s"}`;

    const element = document.createElement("div");
    element.className = "alert";

    element.innerHTML = `
        <div class="alert-symbol">
            ${escapeHtml(alert.symbol)}
        </div>

        <div class="alert-side ${escapeHtml(alert.side)}">
            ${escapeHtml(alert.side)}
        </div>

        <div class="alert-score">
            Z ${Number(alert.z_score).toFixed(2)}
        </div>

        <div>
            $${Number(alert.price).toLocaleString()}
        </div>

        <div class="alert-meta">
            OFI ${Number(alert.signed_ofi).toFixed(6)}
            · Qty ${Number(alert.quantity).toFixed(6)}
        </div>
    `;

    liveAlerts.prepend(element);

    while (liveAlerts.children.length > 20) {
        liveAlerts.lastElementChild.remove();
    }
}


/* =========================================================
   ALERT HISTORY
   ========================================================= */

async function loadHistory() {
    try {
        const params = new URLSearchParams();

        const symbol =
            document.getElementById("filter-symbol").value.trim();

        const fromTime =
            document.getElementById("filter-from").value;

        const toTime =
            document.getElementById("filter-to").value;

        const minScore =
            document.getElementById("filter-min-score").value;

        const maxScore =
            document.getElementById("filter-max-score").value;


        if (symbol) {
            params.set("symbol", symbol);
        }

        if (fromTime) {
            params.set(
                "from_time",
                new Date(fromTime).toISOString()
            );
        }

        if (toTime) {
            params.set(
                "to_time",
                new Date(toTime).toISOString()
            );
        }

        if (minScore !== "") {
            params.set("min_z_score", minScore);
        }

        if (maxScore !== "") {
            params.set("max_z_score", maxScore);
        }

        params.set("limit", "50");
        params.set("offset", "0");


        const response = await fetch(
            `/api/alerts/history?${params.toString()}`
        );

        if (!response.ok) {
            throw new Error(
                `Failed to load alert history (${response.status})`
            );
        }

        const result = await response.json();

        historyBody.innerHTML = "";


        if (!result.items || result.items.length === 0) {
            historyBody.innerHTML = `
                <tr>
                    <td colspan="7" class="empty-state">
                        No records found
                    </td>
                </tr>
            `;
            return;
        }


        result.items.forEach(addHistoryRow);

    } catch (error) {
        console.error("History error:", error);

        historyBody.innerHTML = `
            <tr>
                <td colspan="7" class="empty-state">
                    Failed to load alert history.
                </td>
            </tr>
        `;
    }
}


function addHistoryRow(alert) {
    const row = document.createElement("tr");

    let timestamp = null;

    if (alert.alerted_at) {
        timestamp = new Date(alert.alerted_at);

    } else if (alert.alert_time) {
        timestamp = new Date(alert.alert_time);

    } else if (alert.trade_time_ms) {
        timestamp = new Date(Number(alert.trade_time_ms));

    } else if (alert.trade_time) {
        timestamp = new Date(Number(alert.trade_time));
    }


    const time =
        timestamp && !isNaN(timestamp.getTime())
            ? timestamp.toLocaleString()
            : "—";


    row.innerHTML = `
        <td>${escapeHtml(time)}</td>

        <td>${escapeHtml(alert.symbol)}</td>

        <td class="alert-side ${escapeHtml(alert.side)}">
            ${escapeHtml(alert.side)}
        </td>

        <td>${Number(alert.z_score).toFixed(2)}</td>

        <td>${Number(alert.signed_ofi).toFixed(6)}</td>

        <td>$${Number(alert.price).toLocaleString()}</td>

        <td>${Number(alert.quantity).toFixed(6)}</td>
    `;

    historyBody.appendChild(row);
}


/* =========================================================
   NOTIFICATIONS
   ========================================================= */

function showNotification(alert) {
    const notification = document.createElement("div");

    notification.className = "notification";

    notification.innerHTML = `
        <div class="notification-title">
            TOXICITY ALERT - ${escapeHtml(alert.symbol)}
        </div>

        <div class="notification-details">
            ${escapeHtml(alert.side)}
            · Z ${Number(alert.z_score).toFixed(2)}
            · $${Number(alert.price).toLocaleString()}
        </div>
    `;

    notificationContainer.prepend(notification);

    setTimeout(() => {
        notification.remove();
    }, 6000);
}


async function requestNotificationPermission() {
    if (!("Notification" in window)) {
        return;
    }

    if (Notification.permission === "default") {
        await Notification.requestPermission();
    }
}


function showSystemNotification(alert) {
    if (!("Notification" in window)) {
        return;
    }

    if (Notification.permission !== "granted") {
        return;
    }

    const symbol = alert.symbol || "UNKNOWN";
    const zScore =
        Number(alert.z_score || 0).toFixed(2);

    new Notification(
        `TOXICITY ALERT — ${symbol}`,
        {
            body:
                `${alert.side || ""} · Z ${zScore} · ` +
                `Price ${Number(alert.price || 0).toLocaleString()}`,

            tag:
                `toxicity-${alert.event_id || Date.now()}`
        }
    );
}


/* =========================================================
   ADMIN CONFIGURATION
   ========================================================= */

function authHeaders() {
    if (!authToken) {
        return {};
    }

    return {
        "Authorization": `Bearer ${authToken}`
    };
}


async function loginAdmin() {
    const username =
        document.getElementById("admin-username").value.trim();

    const password =
        document.getElementById("admin-password").value;

    if (!username || !password) {
        document.getElementById("config-status").textContent =
            "Enter username and password";

        return;
    }


    try {
        const response = await fetch("/api/auth/login", {
            method: "POST",

            headers: {
                "Content-Type": "application/json"
            },

            body: JSON.stringify({
                username: username,
                password: password
            })
        });


        if (!response.ok) {
            document.getElementById("config-status").textContent =
                "Login failed";

            return;
        }


        const data = await response.json();

        authToken = data.access_token;

        localStorage.setItem(
            "tofd_admin_token",
            authToken
        );

        document.getElementById("admin-password").value = "";

        document.getElementById("config-status").textContent =
            "Authenticated administrator";

        document.getElementById("config-login-btn").hidden = true;
        document.getElementById("config-logout-btn").hidden = false;
        document.getElementById("config-panel").hidden = false;

        await loadConfigurations();

    } catch (error) {
        console.error("Login error:", error);

        document.getElementById("config-status").textContent =
            "Login failed";
    }
}


async function loadConfigurations() {
    try {
        const response = await fetch(
            "/api/config",
            {
                headers: authHeaders()
            }
        );


        if (response.status === 401) {
            logoutAdmin();
            return;
        }


        if (!response.ok) {
            document.getElementById("config-status").textContent =
                "Failed to load configuration";

            return;
        }


        const configs = await response.json();

        const body =
            document.getElementById("config-body");

        body.innerHTML = "";


        configs.forEach(cfg => {
            const row = document.createElement("tr");

            row.innerHTML = `
                <td>
                    ${escapeHtml(cfg.symbol)}
                </td>

                <td>
                    <input
                        class="cfg-z"
                        type="number"
                        step="0.01"
                        min="0.01"
                        value="${Number(cfg.z_threshold)}"
                    >
                </td>

                <td>
                    <input
                        class="cfg-alpha"
                        type="number"
                        step="0.001"
                        min="0"
                        max="1"
                        value="${Number(cfg.ewma_alpha)}"
                    >
                </td>

                <td>
                    <input
                        class="cfg-min"
                        type="number"
                        min="1"
                        value="${Number(cfg.min_trades)}"
                    >
                </td>

                <td>
                    <button class="cfg-save">
                        Save
                    </button>
                </td>
            `;


            row.querySelector(".cfg-save").onclick =
                async () => {

                    const payload = {
                        symbol: cfg.symbol,

                        z_threshold:
                            Number(
                                row.querySelector(".cfg-z").value
                            ),

                        ewma_alpha:
                            Number(
                                row.querySelector(".cfg-alpha").value
                            ),

                        min_trades:
                            Number(
                                row.querySelector(".cfg-min").value
                            )
                    };


                    try {
                        const save =
                            await fetch(
                                "/api/config",
                                {
                                    method: "POST",

                                    headers: {
                                        "Content-Type":
                                            "application/json",

                                        ...authHeaders()
                                    },

                                    body:
                                        JSON.stringify(payload)
                                }
                            );


                        if (save.status === 401) {
                            logoutAdmin();
                            return;
                        }


                        if (!save.ok) {
                            document
                                .getElementById("config-status")
                                .textContent =
                                "Configuration update failed";

                            return;
                        }


                        document
                            .getElementById("config-status")
                            .textContent =
                            `${cfg.symbol} updated and audited`;

                    } catch (error) {
                        console.error(
                            "Configuration update error:",
                            error
                        );

                        document
                            .getElementById("config-status")
                            .textContent =
                            "Configuration update failed";
                    }
                };


            body.appendChild(row);
        });

    } catch (error) {
        console.error(
            "Configuration loading error:",
            error
        );

        document
            .getElementById("config-status")
            .textContent =
            "Failed to load configuration";
    }
}


function logoutAdmin() {
    authToken = "";

    localStorage.removeItem(
        "tofd_admin_token"
    );

    document.getElementById("config-panel").hidden = true;

    document.getElementById("config-login-btn").hidden = false;

    document.getElementById("config-logout-btn").hidden = true;

    document.getElementById("config-status").textContent =
        "Administrator access required";
}


/* =========================================================
   UTILITY
   ========================================================= */

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}


/* =========================================================
   EVENT HANDLERS
   ========================================================= */

document
    .getElementById("refresh-history")
    .addEventListener(
        "click",
        loadHistory
    );


document
    .getElementById("apply-filters")
    .addEventListener(
        "click",
        loadHistory
    );


document
    .getElementById("clear-filters")
    .addEventListener(
        "click",
        () => {

            document.getElementById(
                "filter-symbol"
            ).value = "";

            document.getElementById(
                "filter-from"
            ).value = "";

            document.getElementById(
                "filter-to"
            ).value = "";

            document.getElementById(
                "filter-min-score"
            ).value = "";

            document.getElementById(
                "filter-max-score"
            ).value = "";

            loadHistory();
        }
    );


/* Configuration buttons */

const configLoginButton =
    document.getElementById("config-login-btn");

if (configLoginButton) {
    configLoginButton.addEventListener(
        "click",
        loginAdmin
    );
}


const configLogoutButton =
    document.getElementById("config-logout-btn");

if (configLogoutButton) {
    configLogoutButton.addEventListener(
        "click",
        logoutAdmin
    );
}


/* =========================================================
   INITIALIZATION
   ========================================================= */

loadHistory();

connectWebSocket();

requestNotificationPermission();


/* Restore previous administrator session */

if (authToken) {
    document.getElementById("config-status").textContent =
        "Checking administrator session...";

    document.getElementById("config-login-btn").hidden = true;
    document.getElementById("config-logout-btn").hidden = false;
    document.getElementById("config-panel").hidden = false;

    loadConfigurations();
}
