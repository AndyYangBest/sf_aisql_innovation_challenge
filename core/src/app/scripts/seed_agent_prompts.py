"""Seed agent prompts into Postgres."""

import asyncio

from sqlalchemy import select

from ..core.db.database import local_session
from ..models.agent_prompt import AgentPrompt
from ..prompts.agent_prompt_templates import (
    AGENT_PROMPT_TEMPLATES,
    AGENT_SYSTEM_PROMPT_TEMPLATES,
)


async def _seed() -> None:
    async with local_session() as session:
        existing = await session.execute(
            select(AgentPrompt.agent_name).where(AgentPrompt.is_active.is_(True))
        )
        existing_names = {row[0] for row in existing.all() if row and row[0]}

        for agent_name, prompt in AGENT_PROMPT_TEMPLATES.items():
            if agent_name in existing_names:
                continue
            record = AgentPrompt()
            record.agent_name = agent_name
            record.prompt = prompt
            record.system_prompt = AGENT_SYSTEM_PROMPT_TEMPLATES.get(agent_name)
            record.is_active = True
            session.add(record)

        await session.commit()


def main() -> None:
    asyncio.run(_seed())


if __name__ == "__main__":
    main()
