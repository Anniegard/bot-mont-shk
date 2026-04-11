from __future__ import annotations

from bot.services.ai.types import AIContextPackage, LLMMessage


def build_messages(
    *,
    question: str,
    history: list[dict[str, str]],
    context_package: AIContextPackage,
) -> list[LLMMessage]:
    messages = [
        LLMMessage(
            role="system",
            content=(
                "Ты AI-помощник внутреннего Telegram-бота для складских и Excel-операций. "
                "Отвечай только по переданному контексту. Не придумывай строки, ШК, суммы, "
                "склады и выводы. Если данных мало, часть контекста обрезана или источник "
                "неполный, прямо скажи об ограничении. Ответ держи коротким, практичным, "
                "на русском языке. Не превращай ответ в общий чат и не уходи в темы вне данных."
            ),
        )
    ]

    for item in history:
        role = item.get("role")
        content = (item.get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            messages.append(LLMMessage(role=role, content=content))

    messages.append(
        LLMMessage(
            role="user",
            content=(
                f"Вопрос администратора:\n{question}\n\n"
                f"Контекст из проектных источников:\n{context_package.text}\n\n"
                "Сформируй ответ в 3 коротких частях:\n"
                "1. Что удалось понять.\n"
                "2. Ключевые факты/цифры.\n"
                "3. Ограничения или что не найдено."
            ),
        )
    )
    return messages
