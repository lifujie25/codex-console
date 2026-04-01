(() => {
    let refs = null;
    let polling = null;

    function queryRefs() {
        const group = document.getElementById('schedule-group');
        if (!group) return null;
        return {
            group,
            cronInput: group.querySelector('#schedule-cron-expression'),
            startBtn: group.querySelector('#schedule-start-btn'),
            stopBtn: group.querySelector('#schedule-stop-btn'),
            statusText: group.querySelector('#schedule-status-text'),
        };
    }

    function formatScheduleTime(isoTime) {
        if (!isoTime) return '-';
        const dt = new Date(isoTime);
        if (Number.isNaN(dt.getTime())) return isoTime;
        return dt.toLocaleString('zh-CN', { hour12: false });
    }

    function updateUI(status) {
        if (!refs) return;
        const modeText = status.registration_mode === 'single' ? '单次' : '批量';
        const cronText = status.schedule_cron || '*/30 * * * *';
        const timezoneText = status.timezone || 'Asia/Shanghai';
        if (refs.cronInput) {
            refs.cronInput.value = cronText;
        }

        if (status.enabled) {
            const nextRunText = formatScheduleTime(status.next_run_at);
            const lastRunText = status.last_run_at ? `，上次触发: ${formatScheduleTime(status.last_run_at)}` : '';
            const activeText = status.active_batch_id
                ? `，当前批量任务: ${status.active_batch_id.slice(0, 8)}...`
                : (status.active_task_uuid ? `，当前单任务: ${status.active_task_uuid.slice(0, 8)}...` : '');
            const errorText = status.last_error ? `，最近错误: ${status.last_error}` : '';
            refs.statusText.textContent = `状态: 已启用 | 模式: ${modeText} | Cron: ${cronText} | 时区: ${timezoneText} | 下次触发: ${nextRunText}${lastRunText}${activeText}${errorText}`;
            refs.stopBtn.disabled = false;
        } else {
            refs.statusText.textContent = `状态: 未启用 | Cron: ${cronText} | 时区: ${timezoneText}`;
            refs.stopBtn.disabled = true;
        }
    }

    async function loadStatus(showErrorToast = false) {
        if (!refs) return;
        try {
            const status = await api.get('/registration/schedule/status');
            updateUI(status);
        } catch (error) {
            if (showErrorToast) {
                toast.error(`获取定时状态失败: ${error.message}`);
            }
        }
    }

    function startPolling() {
        if (polling) {
            clearInterval(polling);
        }
        polling = setInterval(() => {
            loadStatus(false);
        }, 10000);
    }

    async function handleStart() {
        const selectedValue = elements.emailService.value;
        if (!selectedValue) {
            toast.error('请选择一个邮箱服务');
            return;
        }

        if (typeof isOutlookBatchMode !== 'undefined' && isOutlookBatchMode) {
            toast.warning('定时注册暂不支持 Outlook 批量模式，请先切回普通邮箱服务');
            return;
        }

        const scheduleCron = (refs.cronInput?.value || '').trim();
        if (!scheduleCron) {
            toast.error('请填写 5 位 Cron 表达式');
            return;
        }

        const [emailServiceType, serviceId] = selectedValue.split(':');
        const payload = buildCommonRegistrationRequest(emailServiceType, serviceId);
        payload.schedule_cron = scheduleCron;
        payload.run_immediately = true;
        payload.registration_mode = (typeof isBatchMode !== 'undefined' && isBatchMode) ? 'batch' : 'single';

        if (payload.registration_mode === 'batch') {
            payload.count = parseInt(elements.batchCount.value, 10) || 100;
            payload.interval_min = parseInt(elements.intervalMin.value, 10) || 5;
            payload.interval_max = parseInt(elements.intervalMax.value, 10) || 30;
            payload.concurrency = parseInt(elements.concurrencyCount.value, 10) || 1;
            payload.mode = elements.concurrencyMode.value || 'pipeline';
        }

        refs.startBtn.disabled = true;
        try {
            const status = await api.post('/registration/schedule/start', payload);
            updateUI(status);
            if (typeof addLog === 'function') {
                addLog('info', `[系统] 定时注册已启动：Cron=${status.schedule_cron}，时区=${status.timezone}（${status.registration_mode === 'single' ? '单次' : '批量'}）`);
            }
            toast.success('定时注册已启动');
        } catch (error) {
            toast.error(`启动定时注册失败: ${error.message}`);
        } finally {
            refs.startBtn.disabled = false;
        }
    }

    async function handleStop() {
        refs.stopBtn.disabled = true;
        try {
            const status = await api.post('/registration/schedule/stop', {});
            updateUI(status);
            if (typeof addLog === 'function') {
                addLog('info', '[系统] 定时注册已停止');
            }
            toast.info('定时注册已停止');
        } catch (error) {
            toast.error(`停止定时注册失败: ${error.message}`);
            await loadStatus(false);
        }
    }

    function init() {
        const original = document.getElementById('schedule-group');
        if (!original) return;

        const cloned = original.cloneNode(true);
        original.replaceWith(cloned);
        refs = queryRefs();
        if (!refs || !refs.startBtn || !refs.stopBtn || !refs.statusText) return;

        refs.startBtn.addEventListener('click', handleStart);
        refs.stopBtn.addEventListener('click', handleStop);
        loadStatus(false);
        startPolling();
    }

    document.addEventListener('DOMContentLoaded', init);
})();
