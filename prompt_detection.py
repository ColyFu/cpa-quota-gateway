"""Conservative input detection; API user roles are not proof of UI authorship."""
import hashlib
import json
import re
from collections import Counter


# These are observed client-generated envelopes, not general keyword filters.
AUTO_PREFIXES = (
    'Another language model started to solve this problem and produced a summary of its thinking process.',
    'Analyze this rollout and produce JSON with `raw_memory`, `rollout_summary`, and `rollout_slug`',
    '## Memory Writing Agent: Phase 2 (Consolidation)',
    'You are a helpful assistant. You will be presented with a user prompt, and your job is to provide a short title for a task',
    'Side conversation boundary.\n\nEverything before this boundary is inherited history',
)
AUTO_TAGS = ('environment_context', 'turn_aborted', 'system-reminder',
             'user_instructions', 'agents_instructions', 'subagent_notification')


def clean_user_text(value):
    """Remove recognized envelopes while preserving the user's text verbatim."""
    if not isinstance(value, str) or not value.strip():
        return []
    text = value.strip()
    if text.replace('\r\n', '\n').startswith(AUTO_PREFIXES):
        return []
    for tag in AUTO_TAGS:
        if text.startswith('<' + tag + '>'):
            closing = '</' + tag + '>'
            if closing not in text:
                return []
            return clean_user_text(text.split(closing, 1)[1])
    if text.startswith(('# Files mentioned by the user:', '# Files mentioned by user:')):
        for marker in ('## My request for Codex:', '## My request:'):
            if marker in text:
                return clean_user_text(text.split(marker, 1)[1])
        # Attachment descriptions alone are not text typed into the input box.
        return []
    if text.startswith('# Response annotations:'):
        comments = []
        match = re.search(r'<response-annotations>(.*?)</response-annotations>', text, re.S)
        if match:
            try:
                annotations = json.loads(match.group(1))
                if isinstance(annotations, list):
                    comments = [a['annotation'] for a in annotations if isinstance(a, dict)
                                and isinstance(a.get('annotation'), str) and a['annotation'].strip()]
            except ValueError:
                pass
        if '## My request:' in text:
            comments.extend(clean_user_text(text.split('## My request:', 1)[1]))
        return comments
    return [value]


def content_text(content):
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ''
    return '\n'.join(part['text'] for part in content if isinstance(part, dict)
                     and part.get('type') in ('text', 'input_text')
                     and isinstance(part.get('text'), str))


def inspect_input(body):
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        data = {}
    result = {'prompts': [], 'tail_user': False, 'incremental': False, 'marker': '', 'automatic': False}
    if not isinstance(data, dict):
        return result
    messages = None
    if isinstance(data.get('messages'), list):
        messages = data['messages']
    elif isinstance(data.get('input'), str):
        messages = [{'role': 'user', 'content': data['input']}]
    elif isinstance(data.get('input'), list):
        messages = data['input']
    elif isinstance(data.get('contents'), list):
        messages = []
        for message in data['contents']:
            if not isinstance(message, dict):
                continue
            parts = message.get('parts', [])
            content = '\n'.join(p['text'] for p in parts if isinstance(p, dict)
                                and isinstance(p.get('text'), str) and not p.get('thought')) if isinstance(parts, list) else ''
            tool_result = isinstance(parts, list) and any(isinstance(p, dict) and 'functionResponse' in p for p in parts)
            messages.append({'role': message.get('role'), 'content': content, 'tool_result': tool_result})
    elif isinstance(data.get('prompt'), (str, list)):
        prompts = [data['prompt']] if isinstance(data['prompt'], str) else data['prompt']
        messages = [{'role': 'user', 'content': p} for p in prompts if isinstance(p, str)]
    for message in messages or []:
        if not isinstance(message, dict):
            result['tail_user'] = False
            continue
        is_user = message.get('role') == 'user'
        raw = content_text(message.get('content')) if is_user else ''
        cleaned = clean_user_text(raw)
        # A client-generated trailing message is not a new UI input event.
        if is_user and raw and not cleaned:
            result['automatic'] = True
            result['tail_user'] = False
            continue
        parts = message.get('content')
        tool_result = message.get('tool_result') or (isinstance(parts, list) and any(
            isinstance(part, dict) and part.get('type') in ('tool_result', 'function_call_output') for part in parts))
        result['tail_user'] = bool(is_user and cleaned and not tool_result)
        if not tool_result:
            result['prompts'].extend(cleaned)
    previous = data.get('previous_response_id')
    if isinstance(previous, str) and previous:
        result['incremental'] = True
        material = json.dumps([previous, result['prompts']], ensure_ascii=False)
        result['marker'] = hashlib.sha256(material.encode()).hexdigest()
    return result


def prompt_ids(prompts):
    return [hashlib.sha256(text.encode()).hexdigest() for text in prompts]


def detect_input(previous, ids, facts, has_session, historical=False):
    """Return per-request input refs plus compact per-conversation history.

    No result is called confirmed: only a client-side input event can establish
    that text was typed in a UI. Repeated API history must never become new input.
    """
    state = previous or {'history': [], 'seen': {}, 'markers': []}
    history, seen = state['history'], dict(state['seen'])
    markers = list(state.get('markers', []))
    if facts.get('incremental'):
        marker = facts['marker']
        novel = [] if marker in markers else ids
        if marker not in markers:
            markers.append(marker)
            history = history + ids
    else:
        # Occurrence counts retain genuine repeated messages in a full transcript
        # while ignoring old text resurfacing after truncation or compaction.
        counts = Counter()
        novel = []
        for identity in ids:
            counts[identity] += 1
            if counts[identity] > seen.get(identity, 0):
                novel.append(identity)
        history = ids or history
    for identity, count in Counter(history).items():
        seen[identity] = max(seen.get(identity, 0), count)
    updated = {'history': history, 'seen': seen, 'markers': markers}
    if not ids or not novel:
        return [], 'no_new_input', 'automatic_or_repeated_history', updated
    if historical:
        if previous is None:
            return [], 'history_only', 'first_observed_history', updated
        return novel, 'unconfirmed', 'historical_boundary_unavailable', updated
    if not has_session:
        return ids[-1:] if facts['tail_user'] else [], 'unconfirmed', 'session_unavailable', updated
    if not facts['tail_user']:
        return [], 'no_new_input', 'agent_continuation', updated
    if previous is None and len(ids) > 1 and not facts.get('incremental'):
        return ids[-1:], 'unconfirmed', 'first_observed_history', updated
    return novel, 'inferred_input', 'user_message_after_history', updated
