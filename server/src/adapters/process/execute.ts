import type { AdapterExecutionContext, AdapterExecutionResult } from "../types.js";
import {
  asString,
  asNumber,
  asStringArray,
  parseObject,
  buildPaperclipEnv,
  buildInvocationEnvForLogs,
  ensurePathInEnv,
  resolveCommandForLogs,
  runChildProcess,
  readPaperclipIssueWorkModeFromContext,
  stringifyPaperclipWakePayload,
} from "../utils.js";

export async function execute(ctx: AdapterExecutionContext): Promise<AdapterExecutionResult> {
  const { runId, agent, config, context, onLog, onMeta, authToken } = ctx;
  const command = asString(config.command, "");
  if (!command) throw new Error("Process adapter missing command");

  const args = asStringArray(config.args);
  const cwd = asString(config.cwd, process.cwd());
  const envConfig = parseObject(config.env);
  const env: Record<string, string> = { ...buildPaperclipEnv(agent) };
  env.PAPERCLIP_RUN_ID = runId;
  if (authToken) env.PAPERCLIP_API_KEY = authToken;

  const ctxRecord = (context ?? {}) as Record<string, unknown>;
  const readTrimmedString = (value: unknown): string | null =>
    typeof value === "string" && value.trim().length > 0 ? value.trim() : null;

  const wakeTaskId = readTrimmedString(ctxRecord.taskId) ?? readTrimmedString(ctxRecord.issueId);
  if (wakeTaskId) env.PAPERCLIP_TASK_ID = wakeTaskId;

  const wakeReason = readTrimmedString(ctxRecord.wakeReason);
  if (wakeReason) env.PAPERCLIP_WAKE_REASON = wakeReason;

  const wakeCommentId =
    readTrimmedString(ctxRecord.wakeCommentId) ?? readTrimmedString(ctxRecord.commentId);
  if (wakeCommentId) env.PAPERCLIP_WAKE_COMMENT_ID = wakeCommentId;

  const approvalId = readTrimmedString(ctxRecord.approvalId);
  if (approvalId) env.PAPERCLIP_APPROVAL_ID = approvalId;

  const approvalStatus = readTrimmedString(ctxRecord.approvalStatus);
  if (approvalStatus) env.PAPERCLIP_APPROVAL_STATUS = approvalStatus;

  const linkedIssueIds = Array.isArray(ctxRecord.issueIds)
    ? ctxRecord.issueIds.filter(
        (value): value is string => typeof value === "string" && value.trim().length > 0,
      )
    : [];
  if (linkedIssueIds.length > 0) env.PAPERCLIP_LINKED_ISSUE_IDS = linkedIssueIds.join(",");

  const issueWorkMode = readPaperclipIssueWorkModeFromContext(ctxRecord);
  if (issueWorkMode) env.PAPERCLIP_ISSUE_WORK_MODE = issueWorkMode;

  const wakePayloadJson = stringifyPaperclipWakePayload(ctxRecord.paperclipWake);
  if (wakePayloadJson) env.PAPERCLIP_WAKE_PAYLOAD_JSON = wakePayloadJson;

  for (const [k, v] of Object.entries(envConfig)) {
    if (typeof v === "string") env[k] = v;
  }
  const runtimeEnv = ensurePathInEnv({ ...process.env, ...env });
  const resolvedCommand = await resolveCommandForLogs(command, cwd, runtimeEnv);
  const loggedEnv = buildInvocationEnvForLogs(env, {
    runtimeEnv,
    includeRuntimeKeys: ["HOME"],
    resolvedCommand,
  });

  const timeoutSec = asNumber(config.timeoutSec, 0);
  const graceSec = asNumber(config.graceSec, 15);

  if (onMeta) {
    await onMeta({
      adapterType: "process",
      command: resolvedCommand,
      cwd,
      commandArgs: args,
      env: loggedEnv,
    });
  }

  const proc = await runChildProcess(runId, command, args, {
    cwd,
    env,
    timeoutSec,
    graceSec,
    onLog,
  });

  if (proc.timedOut) {
    return {
      exitCode: proc.exitCode,
      signal: proc.signal,
      timedOut: true,
      errorMessage: `Timed out after ${timeoutSec}s`,
    };
  }

  if ((proc.exitCode ?? 0) !== 0) {
    return {
      exitCode: proc.exitCode,
      signal: proc.signal,
      timedOut: false,
      errorMessage: `Process exited with code ${proc.exitCode ?? -1}`,
      resultJson: {
        stdout: proc.stdout,
        stderr: proc.stderr,
      },
    };
  }

  return {
    exitCode: proc.exitCode,
    signal: proc.signal,
    timedOut: false,
    resultJson: {
      stdout: proc.stdout,
      stderr: proc.stderr,
    },
  };
}
