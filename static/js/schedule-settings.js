(() => {
    let refs = null;
    let scheduleDefaultCpaEnabled = null;

    function parseServiceIdList(raw) {
        if (!raw || !raw.trim()) return [];
        return raw
            .split(/[,\s]+/)
            .map(v => parseInt(v.trim(), 10))
            .filter(v => Number.isInteger(v) && v > 0);
    }

    function stringifyServiceIdList(list) {
        if (!Array.isArray(list) || list.length === 0) return '';
        return list.join(',');
    }

    async function hasEnabledCpaServicesForScheduleDefault() {
        if (scheduleDefaultCpaEnabled !== null) {
            return scheduleDefaultCpaEnabled;
        }

        try {
            const services = await api.get('/cpa-services?enabled=true');
            scheduleDefaultCpaEnabled = Array.isArray(services) && services.length > 0;
        } catch (_error) {
            scheduleDefaultCpaEnabled = false;
        }

        return scheduleDefaultCpaEnabled;
    }

    function queryRefs() {
        const form = document.getElementById('schedule-settings-form');
        if (!form) return null;
        return {
            form,
            enabled: form.querySelector('#schedule-enabled'),
            cron: form.querySelector('#schedule-cron-expression-settings'),
            runImmediately: form.querySelector('#schedule-run-immediately'),
            registrationMode: form.querySelector('#schedule-registration-mode'),
            emailServiceType: form.querySelector('#schedule-email-service-type'),
            emailServiceId: form.querySelector('#schedule-email-service-id'),
            batchOptions: form.querySelector('#schedule-batch-options'),
            count: form.querySelector('#schedule-count'),
            concurrency: form.querySelector('#schedule-concurrency'),
            concurrencyMode: form.querySelector('#schedule-concurrency-mode'),
            intervalMin: form.querySelector('#schedule-interval-min'),
            intervalMax: form.querySelector('#schedule-interval-max'),
            autoUploadCpa: form.querySelector('#schedule-auto-upload-cpa'),
            cpaServiceIds: form.querySelector('#schedule-cpa-service-ids'),
            autoUploadSub2api: form.querySelector('#schedule-auto-upload-sub2api'),
            sub2apiServiceIds: form.querySelector('#schedule-sub2api-service-ids'),
            autoUploadTm: form.querySelector('#schedule-auto-upload-tm'),
            tmServiceIds: form.querySelector('#schedule-tm-service-ids'),
            stopBtn: form.querySelector('#schedule-stop-btn-settings'),
            refreshBtn: form.querySelector('#schedule-refresh-btn-settings'),
            statusText: form.querySelector('#schedule-settings-status'),
            timezoneText: form.querySelector('#schedule-settings-timezone'),
        };
    }

    function updateModeVisibility() {
        if (!refs?.registrationMode || !refs.batchOptions) return;
        const isBatch = refs.registrationMode.value === 'batch';
        refs.batchOptions.style.display = isBatch ? 'block' : 'none';
    }

    function formatScheduleStatusTime(iso) {
        if (!iso) return '-';
        const dt = new Date(iso);
        if (Number.isNaN(dt.getTime())) return iso;
        return dt.toLocaleString('zh-CN', { hour12: false });
    }

    function updateStatusText(status) {
        if (!refs?.statusText) return;
        const modeText = status.registration_mode === 'single' ? '单次' : '批量';
        const cronText = status.schedule_cron || '*/30 * * * *';
        const timezoneText = status.timezone || 'Asia/Shanghai';

        if (status.enabled) {
            const nextRun = formatScheduleStatusTime(status.next_run_at);
            const lastRun = status.last_run_at ? `，上次触发: ${formatScheduleStatusTime(status.last_run_at)}` : '';
            const active = status.active_batch_id
                ? `，运行中批量任务: ${status.active_batch_id.slice(0, 8)}...`
                : (status.active_task_uuid ? `，运行中单任务: ${status.active_task_uuid.slice(0, 8)}...` : '');
            const err = status.last_error ? `，最近错误: ${status.last_error}` : '';
            refs.statusText.textContent = `状态: 已启用 | 模式: ${modeText} | Cron: ${cronText} | 下次触发: ${nextRun}${lastRun}${active}${err}`;
        } else {
            refs.statusText.textContent = `状态: 未启用 | Cron: ${cronText}`;
        }

        if (refs.timezoneText) {
            refs.timezoneText.textContent = `时区: ${timezoneText}（容器可通过 TZ 传入）`;
        }

        if (refs.stopBtn) {
            refs.stopBtn.disabled = !status.enabled;
        }
    }

    function applyPayloadToForm(payload = {}) {
        if (!refs) return;
        if (refs.cron && payload.schedule_cron) {
            refs.cron.value = payload.schedule_cron;
        }
        refs.registrationMode.value = payload.registration_mode || 'batch';
        refs.emailServiceType.value = payload.email_service_type || 'tempmail';
        refs.emailServiceId.value = payload.email_service_id ?? '';
        refs.runImmediately.checked = payload.run_immediately !== false;
        refs.count.value = payload.count ?? 100;
        refs.intervalMin.value = payload.interval_min ?? 5;
        refs.intervalMax.value = payload.interval_max ?? 30;
        refs.concurrency.value = payload.concurrency ?? 1;
        refs.concurrencyMode.value = payload.mode || 'pipeline';
        refs.autoUploadCpa.checked = Boolean(payload.auto_upload_cpa);
        refs.cpaServiceIds.value = stringifyServiceIdList(payload.cpa_service_ids || []);
        refs.autoUploadSub2api.checked = Boolean(payload.auto_upload_sub2api);
        refs.sub2apiServiceIds.value = stringifyServiceIdList(payload.sub2api_service_ids || []);
        refs.autoUploadTm.checked = Boolean(payload.auto_upload_tm);
        refs.tmServiceIds.value = stringifyServiceIdList(payload.tm_service_ids || []);
        updateModeVisibility();
    }

    async function loadScheduleSettings(showToast = false) {
        if (!refs) return;
        try {
            const status = await api.get('/registration/schedule/status');
            const payload = { ...(status.payload || {}) };
            if (payload.auto_upload_cpa === undefined) {
                payload.auto_upload_cpa = await hasEnabledCpaServicesForScheduleDefault();
            }
            refs.enabled.checked = Boolean(status.enabled);
            refs.cron.value = status.schedule_cron || '*/30 * * * *';
            applyPayloadToForm(payload);
            updateStatusText(status);
        } catch (error) {
            if (showToast) {
                toast.error('加载定时任务配置失败: ' + error.message);
            }
        }
    }

    function buildPayload() {
        const payload = {
            schedule_cron: (refs.cron.value || '').trim() || '*/30 * * * *',
            run_immediately: refs.runImmediately.checked,
            registration_mode: refs.registrationMode.value || 'batch',
            email_service_type: refs.emailServiceType.value || 'tempmail',
            auto_upload_cpa: refs.autoUploadCpa.checked,
            cpa_service_ids: parseServiceIdList(refs.cpaServiceIds.value),
            auto_upload_sub2api: refs.autoUploadSub2api.checked,
            sub2api_service_ids: parseServiceIdList(refs.sub2apiServiceIds.value),
            auto_upload_tm: refs.autoUploadTm.checked,
            tm_service_ids: parseServiceIdList(refs.tmServiceIds.value),
        };

        const emailServiceId = (refs.emailServiceId.value || '').trim();
        if (emailServiceId) {
            payload.email_service_id = parseInt(emailServiceId, 10);
        }

        if (payload.registration_mode === 'batch') {
            payload.count = parseInt(refs.count.value, 10) || 100;
            payload.interval_min = parseInt(refs.intervalMin.value, 10) || 5;
            payload.interval_max = parseInt(refs.intervalMax.value, 10) || 30;
            payload.concurrency = parseInt(refs.concurrency.value, 10) || 1;
            payload.mode = refs.concurrencyMode.value || 'pipeline';
        }

        return payload;
    }

    async function handleSave(e) {
        e.preventDefault();

        if (!refs.enabled.checked) {
            await handleStop(null, false);
            toast.success('定时任务已禁用');
            return;
        }

        try {
            const payload = buildPayload();
            const status = await api.post('/registration/schedule/start', payload);
            refs.enabled.checked = true;
            refs.cron.value = status.schedule_cron || payload.schedule_cron;
            applyPayloadToForm(status.payload || payload);
            updateStatusText(status);
            toast.success('定时任务已保存并启用');
        } catch (error) {
            toast.error('保存定时任务失败: ' + error.message);
        }
    }

    async function handleStop(_evt = null, showToast = true) {
        try {
            const status = await api.post('/registration/schedule/stop', {});
            refs.enabled.checked = false;
            updateStatusText(status);
            if (showToast) {
                toast.info('定时任务已停止');
            }
        } catch (error) {
            if (showToast) {
                toast.error('停止定时任务失败: ' + error.message);
            }
        }
    }

    function init() {
        const original = document.getElementById('schedule-settings-form');
        if (!original) return;

        const cloned = original.cloneNode(true);
        original.replaceWith(cloned);
        refs = queryRefs();
        if (!refs) return;

        refs.form.addEventListener('submit', handleSave);
        refs.stopBtn.addEventListener('click', handleStop);
        refs.refreshBtn.addEventListener('click', () => loadScheduleSettings(true));
        refs.registrationMode.addEventListener('change', updateModeVisibility);

        loadScheduleSettings(false);
    }

    document.addEventListener('DOMContentLoaded', init);
})();
