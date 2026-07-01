import os
import asyncio
import logging
from typing import Optional
import asyncpg
import discord
from discord import app_commands
from dotenv import load_dotenv

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("InviteTrackerBot")

# .env 파일 로드
load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("SERVER_ID")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")


class InviteTrackerBot(discord.Client):
    def __init__(self, **options):
        super().__init__(**options)
        # 초대 캐시: {guild_id: {invite_code: uses_count}}
        self.invites = {}
        # 동시 유입 처리를 위한 동시성 Lock
        self.db_lock = asyncio.Lock()
        self.tree = app_commands.CommandTree(self)
        self.tree.on_error = self.on_tree_error
        self.synced = False
        self.db_pool = None

    async def initialize_db(self):
        """Supabase PostgreSQL 데이터베이스 및 테이블을 초기화합니다."""
        if not SUPABASE_DB_URL:
            logger.critical("SUPABASE_DB_URL environment variable is missing!")
            return
            
        try:
            self.db_pool = await asyncpg.create_pool(SUPABASE_DB_URL)
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS referrals (
                        guild_id TEXT,
                        user_id TEXT,
                        invite_code TEXT,
                        join_count INTEGER DEFAULT 0,
                        leave_count INTEGER DEFAULT 0,
                        PRIMARY KEY (guild_id, user_id, invite_code)
                    );
                    CREATE TABLE IF NOT EXISTS joined_members (
                        guild_id TEXT,
                        user_id TEXT,
                        inviter_id TEXT,
                        invite_code TEXT,
                        PRIMARY KEY (guild_id, user_id)
                    );
                    CREATE TABLE IF NOT EXISTS guild_settings (
                        guild_id TEXT PRIMARY KEY,
                        log_enabled INTEGER DEFAULT 1,
                        log_channel_id TEXT
                    );
                """)
                print("[Info] Supabase Database & Tables initialized successfully.")
        except Exception as e:
            logger.exception("Failed to initialize database")

    async def update_referrals_db(self, guild_id, member_id, inviter_id, invite_code):
        """Supabase DB에 초대 횟수를 기록하고, 전체 누적 성공 횟수를 조회하여 반환합니다."""
        if not self.db_pool:
            return 0, 0, 0, inviter_id, invite_code, True
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # 이미 가입되어 있는지 확인
                    row = await conn.fetchrow("""
                        SELECT inviter_id, invite_code FROM joined_members WHERE guild_id = $1 AND user_id = $2
                    """, str(guild_id), str(member_id))
                    
                    if row:
                        orig_inviter_id, orig_invite_code = row['inviter_id'], row['invite_code']
                        
                        # 1. 동일한 초대자로 재입장한 경우
                        if str(orig_inviter_id) == str(inviter_id):
                            # 기존 초대자의 leave_count를 1 차감 (최소 0)
                            await conn.execute("""
                                UPDATE referrals
                                SET leave_count = CASE WHEN leave_count > 0 THEN leave_count - 1 ELSE 0 END
                                WHERE guild_id = $1 AND user_id = $2 AND invite_code = $3
                            """, str(guild_id), str(orig_inviter_id), orig_invite_code)

                            r = await conn.fetchrow("""
                                SELECT SUM(join_count) as joins, SUM(leave_count) as leaves FROM referrals
                                WHERE guild_id = $1 AND user_id = $2
                            """, str(guild_id), str(orig_inviter_id))
                            joins = r['joins'] if (r and r['joins'] is not None) else 0
                            leaves = r['leaves'] if (r and r['leaves'] is not None) else 0
                            return joins - leaves, joins, leaves, orig_inviter_id, orig_invite_code, False
                        
                        # 2. 다른 초대자로 재입장한 경우 (초대 실적을 신규 초대자에게 이전)
                        else:
                            # 기존 초대자의 실적 차감
                            await conn.execute("""
                                UPDATE referrals
                                SET join_count = CASE WHEN join_count > 0 THEN join_count - 1 ELSE 0 END,
                                    leave_count = CASE WHEN leave_count > 0 THEN leave_count - 1 ELSE 0 END
                                WHERE guild_id = $1 AND user_id = $2 AND invite_code = $3
                            """, str(guild_id), str(orig_inviter_id), orig_invite_code)

                            # 신규 초대자의 join_count 1 증가
                            await conn.execute("""
                                INSERT INTO referrals (guild_id, user_id, invite_code, join_count, leave_count)
                                VALUES ($1, $2, $3, 1, 0)
                                ON CONFLICT(guild_id, user_id, invite_code)
                                DO UPDATE SET join_count = referrals.join_count + 1
                            """, str(guild_id), str(inviter_id), invite_code)

                            # 가입 정보 테이블 업데이트 (초대자를 신규 초대자로 갱신)
                            await conn.execute("""
                                UPDATE joined_members
                                SET inviter_id = $1, invite_code = $2
                                WHERE guild_id = $3 AND user_id = $4
                            """, str(inviter_id), invite_code, str(guild_id), str(member_id))

                            r = await conn.fetchrow("""
                                SELECT SUM(join_count) as joins, SUM(leave_count) as leaves FROM referrals
                                WHERE guild_id = $1 AND user_id = $2
                            """, str(guild_id), str(inviter_id))
                            joins = r['joins'] if (r and r['joins'] is not None) else 0
                            leaves = r['leaves'] if (r and r['leaves'] is not None) else 0
                            return joins - leaves, joins, leaves, inviter_id, invite_code, True

                    # 관계 기록 (최초 가입)
                    await conn.execute("""
                        INSERT INTO joined_members (guild_id, user_id, inviter_id, invite_code)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (guild_id, user_id)
                        DO UPDATE SET inviter_id = EXCLUDED.inviter_id, invite_code = EXCLUDED.invite_code
                    """, str(guild_id), str(member_id), str(inviter_id), invite_code)

                    # UPSERT를 통한 join_count 1 증가 처리
                    await conn.execute("""
                        INSERT INTO referrals (guild_id, user_id, invite_code, join_count, leave_count)
                        VALUES ($1, $2, $3, 1, 0)
                        ON CONFLICT(guild_id, user_id, invite_code)
                        DO UPDATE SET join_count = referrals.join_count + 1
                    """, str(guild_id), str(inviter_id), invite_code)

                # 누적 가입 인원 합산 조회
                row = await conn.fetchrow("""
                    SELECT SUM(join_count) as joins, SUM(leave_count) as leaves FROM referrals
                    WHERE guild_id = $1 AND user_id = $2
                """, str(guild_id), str(inviter_id))
                joins = row['joins'] if (row and row['joins'] is not None) else 0
                leaves = row['leaves'] if (row and row['leaves'] is not None) else 0
                total_sum = joins - leaves
                print(f"[Success] DB updated for User {inviter_id} (Code: {invite_code}), Total: {total_sum}")
                return total_sum, joins, leaves, inviter_id, invite_code, True
        except Exception as e:
            logger.exception(f"Failed to update database for inviter {inviter_id}")
            return 0, 0, 0, inviter_id, invite_code, True

    async def decrement_referrals_db(self, guild_id, member_id):
        """멤버가 나갔을 때 초대한 사람의 추천 수를 1 감소시키고 기록을 유지합니다."""
        if not self.db_pool:
            return None
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # 누가 초대했는지 조회
                    row = await conn.fetchrow("""
                        SELECT inviter_id, invite_code FROM joined_members
                        WHERE guild_id = $1 AND user_id = $2
                    """, str(guild_id), str(member_id))
                    if not row:
                        return None
                    inviter_id, invite_code = row['inviter_id'], row['invite_code']

                    # 추천 퇴장 수 증가
                    await conn.execute("""
                        UPDATE referrals
                        SET leave_count = leave_count + 1
                        WHERE guild_id = $1 AND user_id = $2 AND invite_code = $3
                    """, str(guild_id), str(inviter_id), invite_code)
                
                # 누적 횟수 조회
                row = await conn.fetchrow("""
                    SELECT SUM(join_count) as joins, SUM(leave_count) as leaves FROM referrals
                    WHERE guild_id = $1 AND user_id = $2
                """, str(guild_id), str(inviter_id))
                joins = row['joins'] if (row and row['joins'] is not None) else 0
                leaves = row['leaves'] if (row and row['leaves'] is not None) else 0
                total_sum = joins - leaves
                return {"inviter_id": inviter_id, "total": total_sum, "joins": joins, "leaves": leaves}
        except Exception as e:
            logger.exception(f"Failed to decrement database for member {member_id}")
            return None

    async def get_guild_settings(self, guild_id):
        """서버의 로그 설정을 가져옵니다. 설정이 없으면 기본값을 반환합니다."""
        if not self.db_pool:
            return {"log_enabled": True, "log_channel_id": None}
        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT log_enabled, log_channel_id FROM guild_settings WHERE guild_id = $1", str(guild_id))
                if row:
                    return {"log_enabled": bool(row['log_enabled']), "log_channel_id": row['log_channel_id']}
        except Exception as e:
            logger.exception(f"Failed to get guild settings for guild {guild_id}")
        return {"log_enabled": True, "log_channel_id": None}

    async def update_guild_settings(self, guild_id, log_enabled, log_channel_id):
        """서버의 로그 설정을 업데이트합니다."""
        if not self.db_pool:
            return False
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO guild_settings (guild_id, log_enabled, log_channel_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT(guild_id)
                    DO UPDATE SET log_enabled = EXCLUDED.log_enabled, log_channel_id = EXCLUDED.log_channel_id
                """, str(guild_id), 1 if log_enabled else 0, str(log_channel_id) if log_channel_id else None)
                return True
        except Exception as e:
            logger.exception(f"Failed to update guild settings for guild {guild_id}")
            return False

    async def get_leaderboard(self, guild_id, limit=10, offset=0):
        """서버의 초대 리더보드 데이터를 페이징 처리하여 조회합니다."""
        if not self.db_pool:
            return []
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT user_id, SUM(join_count) as joins, SUM(leave_count) as leaves
                    FROM referrals
                    WHERE guild_id = $1
                    GROUP BY user_id
                    ORDER BY (SUM(join_count) - SUM(leave_count)) DESC, SUM(join_count) DESC
                    LIMIT $2 OFFSET $3
                """, str(guild_id), limit, offset)
                leaderboard = []
                for row in rows:
                    user_id = row['user_id']
                    joins = row['joins'] if row['joins'] is not None else 0
                    leaves = row['leaves'] if row['leaves'] is not None else 0
                    net = joins - leaves
                    leaderboard.append({
                        "user_id": user_id,
                        "joins": joins,
                        "leaves": leaves,
                        "net": net
                    })
                return leaderboard
        except Exception as e:
            logger.exception(f"Failed to fetch leaderboard for guild {guild_id}")
            return []

    async def cache_invites(self, guild):
        """서버의 모든 초대 코드를 캐싱합니다."""
        try:
            if guild.me.guild_permissions.manage_guild:
                invs = await guild.invites()
                self.invites[guild.id] = {invite.code: invite.uses for invite in invs}
                logger.info(f"Cached {len(invs)} invites for guild: {guild.name} ({guild.id})")
            else:
                logger.warning(f"Cannot cache invites for guild: {guild.name} ({guild.id}) due to lack of 'Manage Server' permission.")
        except Exception as e:
            logger.exception(f"Failed to cache invites for guild {guild.id}")

    async def on_ready(self):
        logger.info(f"{self.user} has connected to Discord!")
        # 데이터베이스 초기화
        await self.initialize_db()
        # 시작 시 모든 서버의 초대 링크 캐싱
        for guild in self.guilds:
            await self.cache_invites(guild)
        
        # 중복 동기화 방지 및 슬래시 커맨드 동기화
        if not self.synced:
            # SERVER_ID(GUILD_ID)가 지정되어 있으면 해당 특정 길드만 빠르게 싱크 처리
            if GUILD_ID:
                try:
                    target_guild = self.get_guild(int(GUILD_ID))
                    if target_guild:
                        self.tree.copy_global_to(guild=target_guild)
                        synced = await self.tree.sync(guild=target_guild)
                        logger.info(f"Synced {len(synced)} command(s) to target guild: {target_guild.name} ({target_guild.id})")
                        self.synced = True
                except Exception as e:
                    logger.exception(f"Failed to sync commands to target guild {GUILD_ID}")
            
            # 특정 서버 지정이 없거나 동기화에 실패한 경우 전체 서버 동기화 시도
            if not self.synced:
                for guild in self.guilds:
                    try:
                        self.tree.copy_global_to(guild=guild)
                        synced = await self.tree.sync(guild=guild)
                        logger.info(f"Synced {len(synced)} command(s) to guild: {guild.name} ({guild.id})")
                    except Exception as e:
                        logger.exception(f"Failed to sync commands to guild {guild.id}")
                self.synced = True

        logger.info("Ready to track invites.")

    async def on_message(self, message):
        # 봇의 메시지는 무시
        if message.author.bot:
            return

        # '!!sync' 입력 시 수동 강제 동기화 수행
        if message.content == "!!sync":
            if message.guild and message.author.guild_permissions.administrator:
                try:
                    await message.channel.send("🔄 슬래시 명령어를 동기화하는 중입니다...")
                    self.tree.copy_global_to(guild=message.guild)
                    synced = await self.tree.sync(guild=message.guild)
                    await message.channel.send(f"✅ 현재 서버({message.guild.name})에 {len(synced)}개의 명령어를 동기화 완료했습니다.")
                except Exception as e:
                    logger.exception("Failed to sync commands via message command")
                    await message.channel.send(f"❌ 동기화 중 오류가 발생했습니다: {e}")
            elif not message.guild:
                await message.channel.send("❌ 서버 채널 내에서만 사용 가능합니다.")
            else:
                await message.channel.send("❌ 이 명령어는 관리자만 사용할 수 있습니다.")
 
        # '!!clear' 입력 시 현재 길드 및 글로벌에 동기화된 슬래시 커맨드 캐시를 초기화합니다.
        elif message.content == "!!clear":
            if message.guild and message.author.guild_permissions.administrator:
                try:
                    await message.channel.send("🔄 등록된 모든 슬래시 명령어를 청소하는 중입니다...")
                    # 1. 현재 서버의 길드 명령어 삭제
                    self.tree.clear_commands(guild=message.guild)
                    await self.tree.sync(guild=message.guild)
                    
                    # 2. 글로벌 명령어 삭제 (메모리에서도 소거되므로 봇 재부팅 필요)
                    self.tree.clear_commands(guild=None)
                    await self.tree.sync()
                    
                    await message.channel.send(
                        "✅ **명령어 청소가 완료되었습니다.**\n"
                        "⚠️ **[주의]** 메모리 상의 슬래시 명령어 트리도 초기화되었으므로, **봇 프로그램을 완전히 종료(Ctrl + C)한 뒤 다시 켜주셔야(재부팅)** 명령어가 복구됩니다.\n"
                        "봇을 다시 켜신 후 디스코드를 새로고침(Ctrl + R) 하고 **!!sync**를 실행해 주시기 바랍니다."
                    )
                except Exception as e:
                    logger.exception("Failed to clear commands")
                    await message.channel.send(f"❌ 명령어 청소 중 오류가 발생했습니다: {e}")
            elif not message.guild:
                await message.channel.send("❌ 서버 채널 내에서만 사용 가능합니다.")
            else:
                await message.channel.send("❌ 이 명령어는 관리자만 사용할 수 있습니다.")


    async def on_interaction(self, interaction: discord.Interaction):
        # 컴포넌트(버튼) 클릭 인터랙션인지 확인
        if interaction.type == discord.InteractionType.component:
            custom_id = interaction.data.get("custom_id", "")
            if custom_id.startswith("entry_role:"):
                parts = custom_id.split(":")
                try:
                    role_id = int(parts[1])
                    remove_role_id = int(parts[2]) if len(parts) > 2 else 0
                except ValueError:
                    return
                
                guild = interaction.guild
                if not guild:
                    return
                
                role = guild.get_role(role_id)
                if not role:
                    await interaction.response.send_message("❌ 설정된 역할을 서버에서 찾을 수 없습니다.", ephemeral=True)
                    return
                
                # 추가할 역할이 이미 있고, 제거할 역할은 유저에게 없는 경우
                has_target_role = role in interaction.user.roles
                has_remove_role = False
                if remove_role_id > 0:
                    remove_role = guild.get_role(remove_role_id)
                    if remove_role and remove_role in interaction.user.roles:
                        has_remove_role = True

                if has_target_role and not has_remove_role:
                    await interaction.response.send_message(" 이미 역할을 가지고 있습니다.", ephemeral=True)
                    return
                
                try:
                    roles_to_add = []
                    roles_to_remove = []
                    
                    if not has_target_role:
                        roles_to_add.append(role)
                        
                    removed_text = ""
                    if remove_role_id > 0:
                        remove_role = guild.get_role(remove_role_id)
                        if remove_role and remove_role in interaction.user.roles:
                            roles_to_remove.append(remove_role)
                            removed_text = f", {remove_role.name} 역할이 제거되었습니다"
                    
                    if roles_to_add:
                        await interaction.user.add_roles(*roles_to_add)
                    if roles_to_remove:
                        await interaction.user.remove_roles(*roles_to_remove)
                        
                    await interaction.response.send_message(f"✅ {role.name} 역할이 부여되었습니다{removed_text}. 서버 입장을 환영합니다!", ephemeral=True)
                except discord.Forbidden:
                    await interaction.response.send_message("❌ 봇에게 역할 관리 권한이 없습니다. (봇의 역할 순위가 해당 역할들보다 높아야 합니다.)", ephemeral=True)
                except Exception as e:
                    await interaction.response.send_message(f"❌ 역할 처리 중 오류가 발생했습니다: {e}", ephemeral=True)

    async def on_tree_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        # 파라미터 변환 오류 (TransformerError)
        if isinstance(error, app_commands.errors.TransformerError):
            await interaction.response.send_message(
                f"❌ 입력값을 올바르게 변환하지 못했습니다.\n"
                f"• 입력한 값: `{error.value}`\n"
                f"• 요구되는 형식: `텍스트 채널 (#채널명)`\n"
                f"• 해결 방법: 채널을 직접 타이핑하여 보내는 대신, 디스코드 자동 완성 목록에서 제안하는 채널을 마우스로 클릭하여 선택해 주시기 바랍니다.",
                ephemeral=True
            )
        # 권한 부족 오류 (MissingPermissions)
        elif isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message("❌ 이 명령어를 실행할 권한이 없습니다.", ephemeral=True)
        # 그 외 오류
        else:
            print(f"[Error] Command tree error: {error}")
            try:
                await interaction.response.send_message(f"❌ 명령어 실행 중 오류가 발생했습니다: {error}", ephemeral=True)
            except Exception:
                pass

    async def on_guild_join(self, guild):
        # 새로운 서버에 들어갔을 때 캐싱
        await self.cache_invites(guild)

    async def on_guild_remove(self, guild):
        # 서버에서 나갔을 때 캐시 삭제
        self.invites.pop(guild.id, None)

    async def on_member_remove(self, member):
        guild = member.guild
        if member.bot:
            return

        async with self.db_lock:
            res = await self.decrement_referrals_db(guild.id, member.id)
            if res:
                print(f"[Leave] {member.name} left. Decremented inviter {res['inviter_id']} total to {res['total']} (Joins: {res['joins']}, Leaves: {res['leaves']})")

    async def on_invite_create(self, invite):
        # 새로운 초대 링크 생성 시 캐시 추가
        guild_id = invite.guild.id
        if guild_id not in self.invites:
            self.invites[guild_id] = {}
        self.invites[guild_id][invite.code] = invite.uses
        print(f"[Info] Invite created: {invite.code} in guild {invite.guild.name}")

    async def on_invite_delete(self, invite):
        # 초대 링크 삭제 시 캐시 제거
        guild_id = invite.guild.id
        if guild_id in self.invites:
            self.invites[guild_id].pop(invite.code, None)
            print(f"[Info] Invite deleted: {invite.code} in guild {invite.guild.name}")

    async def on_member_join(self, member):
        guild = member.guild
        
        # 봇 자신은 제외
        if member.bot:
            return

        # 동시 입장 처리를 위해 전체 초대 분석 및 DB 업데이트 구간을 Lock으로 보호
        async with self.db_lock:
            used_invite = None
            try:
                # 멤버 가입 시점의 최신 초대 코드 가져오기
                new_invites = await guild.invites()
            except Exception as e:
                print(f"[Error] Failed to fetch invites on member join in guild {guild.id}: {e}")
                return

            old_invites = self.invites.get(guild.id, {})

            # 사용 횟수(uses)가 증가한 초대 코드를 찾음
            for invite in new_invites:
                old_uses = old_invites.get(invite.code, 0)
                if invite.uses > old_uses:
                    used_invite = invite
                    break

            # 초대 코드가 매칭되었다면
            if used_invite:
                inviter = used_invite.inviter
                invite_code = used_invite.code

                # 초대한 사람 정보가 있는 경우
                if inviter:
                    inviter_id = inviter.id
                    inviter_mention = inviter.mention
                    
                    # DB 업데이트 및 총 카운트 획득
                    total, joins, leaves, actual_inviter_id, actual_invite_code, is_new = await self.update_referrals_db(guild.id, member.id, inviter_id, invite_code)
                    
                    settings = await self.get_guild_settings(guild.id)
                    if not settings["log_enabled"]:
                        print(f"[Info] Welcome log is disabled for guild {guild.id}")
                    else:
                        # 채널 결정 (DB 설정 -> 시스템 채널 -> 첫 번째 전송 가능 채널)
                        channel = None
                        db_channel_id = settings.get("log_channel_id")
                        if db_channel_id:
                            try:
                                channel = guild.get_channel(int(db_channel_id))
                            except ValueError:
                                pass
                        if not channel:
                            channel = guild.system_channel
                        if not channel:
                            for text_channel in guild.text_channels:
                                if text_channel.permissions_for(guild.me).send_messages:
                                    channel = text_channel
                                    break
  
                        if channel:
                            if is_new:
                                msg_content = (
                                    f"🎉 {member.mention}님이 <@{actual_inviter_id}>님이 생성한 초대 코드({actual_invite_code})로 입장하셨습니다!\n"
                                    f"📈 (현재 누적 추천 성공: {total}회 | 유입: {joins} | 퇴장: {leaves})"
                                )
                            else:
                                msg_content = (
                                    f"🎉 {member.mention}님이 다시 입장하셨습니다! (기존 초대자: <@{actual_inviter_id}>)\n"
                                    f"📈 (현재 누적 추천 성공: {total}회 | 유입: {joins} | 퇴장: {leaves})"
                                )
                            try:
                                await channel.send(content=msg_content)
                                print(f"[Success] Sent welcome message to {channel.name}")
                            except Exception as e:
                                print(f"[Error] Failed to send message: {e}")
                        else:
                            print(f"[Warning] No suitable channel found to send welcome message in guild {guild.name}")
                else:
                    print(f"[Warning] Invite {invite_code} used, but inviter is None.")
            else:
                # 초대 코드를 찾을 수 없는 경우
                print(f"[Warning] Member {member.name} joined, but no matching invite link was found.")

            # 가입 완료 후 초대장 캐시 최신화 (Lock 내부에서 갱신하여 순서 보장)
            self.invites[guild.id] = {invite.code: invite.uses for invite in new_invites}

class LeaderboardView(discord.ui.View):
    def __init__(self, bot, guild, author_id, limit=10):
        super().__init__(timeout=60.0)
        self.bot = bot
        self.guild = guild
        self.author_id = author_id
        self.limit = limit
        self.page = 0

    async def generate_embed(self):
        offset = self.page * self.limit
        leaderboard_data = await self.bot.get_leaderboard(self.guild.id, limit=self.limit, offset=offset)
        
        embed = discord.Embed(
            title=f"🏆 {self.guild.name} 초대 순위 (페이지 {self.page + 1})",
            color=discord.Color.gold(),
            timestamp=discord.utils.utcnow()
        )
        if self.guild.icon:
            embed.set_thumbnail(url=self.guild.icon.url)

        if not leaderboard_data:
            embed.description = "ℹ️ 이 페이지에는 초대 기록이 없습니다."
            return embed, False

        next_data = await self.bot.get_leaderboard(self.guild.id, limit=1, offset=offset + self.limit)
        has_next = len(next_data) > 0

        description_lines = []
        medals = ["🥇", "🥈", "🥉"]

        for i, data in enumerate(leaderboard_data):
            rank = offset + i + 1
            rank_prefix = medals[rank - 1] if rank <= 3 else f"**{rank}위.**"
            user_mention = f"<@{data['user_id']}>"
            line = (
                f"{rank_prefix} {user_mention} — **{data['net']}명** 성공\n"
                f"   └ (가입: {data['joins']}명 | 퇴장: {data['leaves']}명)"
            )
            description_lines.append(line)

        embed.description = "\n\n".join(description_lines)
        return embed, has_next

    async def update_view(self, interaction: discord.Interaction):
        embed, has_next = await self.generate_embed()
        
        self.prev_button.disabled = self.page == 0
        self.next_button.disabled = not has_next
        
        await interaction.response.edit_message(embed=embed, view=self)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ 이 리더보드 제어판은 명령어 요청자만 사용할 수 있습니다.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="이전 페이지", style=discord.ButtonStyle.secondary, emoji="⬅️")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
            await self.update_view(interaction)

    @discord.ui.button(label="다음 페이지", style=discord.ButtonStyle.secondary, emoji="➡️")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page += 1
        await self.update_view(interaction)


class SettingsGroup(app_commands.Group):
    def __init__(self, bot):
        super().__init__(name="설정", description="서버 설정을 관리합니다.")
        self.bot = bot

    @app_commands.command(name="로그채널", description="신규 유저 입장 로그 설정을 변경합니다.")
    @app_commands.describe(
        enabled="로그 전송 기능의 활성화 여부를 선택하세요.",
        channel="로그를 전송할 채널을 선택하세요. (지정하지 않으면 자동 선택 또는 시스템 채널 사용)"
    )
    @app_commands.default_permissions(administrator=True)
    async def set_welcome_log(self, interaction: discord.Interaction, enabled: bool, channel: Optional[discord.TextChannel] = None):
        # guild 체크를 가장 먼저 수행
        if interaction.guild is None:
            await interaction.response.send_message("❌ 서버 내에서만 사용 가능한 명령어입니다.", ephemeral=True)
            return

        # 관리자 권한 확인
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ 이 명령어는 관리자만 사용할 수 있습니다.", ephemeral=True)
            return

        guild_id = interaction.guild.id
        channel_id = str(channel.id) if channel else None
        success = await self.bot.update_guild_settings(guild_id, enabled, channel_id)
        if success:
            status_text = "활성화" if enabled else "비활성화"
            if channel:
                channel_mention = channel.mention
            else:
                channel_mention = "자동 선택 (시스템 채널 등)"
            await interaction.response.send_message(
                f"✅ **입장 로그 설정이 변경되었습니다.**\n"
                f"• **상태**: {status_text}\n"
                f"• **로그 채널**: {channel_mention}",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("❌ 설정 변경 중 오류가 발생했습니다. DB 로그를 확인해 주세요.", ephemeral=True)

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("[Critical] DISCORD_TOKEN is missing. Please set it in .env file.")
    else:
        # 인텐트 설정
        intents = discord.Intents.default()
        intents.members = True
        intents.invites = True
        intents.message_content = True
        
        bot = InviteTrackerBot(intents=intents)
        bot.tree.add_command(SettingsGroup(bot))

        # 슬래시 커맨드 정의
        @bot.tree.command(name="서버입장생성", description="서버 입장 안내 임베드와 역할 부여 버튼을 생성합니다.")
        @app_commands.describe(
            role="입장 시 부여할 역할을 선택하세요.",
            channel="안내 메시지를 생성할 채널을 선택하세요. (지정하지 않으면 현재 채널)",
            remove_role="입장 시 제거할 역할을 선택하세요. (선택사항)"
        )
        @app_commands.default_permissions(administrator=True)
        async def server_entry_setup(
            interaction: discord.Interaction, 
            role: discord.Role, 
            channel: Optional[discord.TextChannel] = None, 
            remove_role: Optional[discord.Role] = None
        ):
            # guild 체크를 가장 먼저 수행
            if interaction.guild is None:
                await interaction.response.send_message("❌ 서버 내에서만 사용 가능한 명령어입니다.", ephemeral=True)
                return

            # 관리자 권한 확인
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("❌ 이 명령어는 관리자만 사용할 수 있습니다.", ephemeral=True)
                return

            try:
                target_channel = channel if channel else interaction.channel
                embed = discord.Embed(
                    title="👋 서버 입장 안내",
                    description="버튼을 클릭하시면 서버에 입장됩니다.",
                    color=discord.Color.green()
                )
                
                # 지속형 버튼을 위해 custom_id에 역할 ID 및 제거할 역할 ID를 포함
                view = discord.ui.View(timeout=None)
                
                custom_id = f"entry_role:{role.id}"
                if remove_role:
                    custom_id += f":{remove_role.id}"
                    
                button = discord.ui.Button(
                    label="서버 입장",
                    style=discord.ButtonStyle.green,
                    custom_id=custom_id
                )
                view.add_item(button)
                
                # 봇의 대상 채널 메시지/임베드 쓰기 권한 점검
                permissions = target_channel.permissions_for(interaction.guild.me)
                if not permissions.send_messages or not permissions.embed_links:
                    await interaction.response.send_message(
                        f"❌ 봇이 {target_channel.mention} 채널에 메시지 또는 임베드를 보낼 권한이 없습니다. 채널 권한 설정을 확인해 주세요.",
                        ephemeral=True
                    )
                    return

                await interaction.response.send_message(f"✅ {target_channel.mention} 채널에 안내 메시지가 생성되었습니다.", ephemeral=True)
                await target_channel.send(embed=embed, view=view)
            except discord.Forbidden:
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message(
                            "❌ 봇의 권한이 부족합니다. 해당 채널을 볼 수 있는 권한(View Channel)과 메시지 전송(Send Messages) 권한이 봇에게 있는지 확인해 주세요.",
                            ephemeral=True
                        )
                    else:
                        await interaction.followup.send(
                            "❌ 봇의 권한이 부족하여 메시지를 보낼 수 없습니다. (Forbidden: 403 / Missing Access)",
                            ephemeral=True
                        )
                except Exception as ex:
                    logger.exception(f"Failed to send permission error response: {ex}")
            except Exception as e:
                logger.exception(f"Exception in server_entry_setup: {e}")
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message(f"❌ 오류가 발생했습니다: {e}", ephemeral=True)
                    else:
                        await interaction.followup.send(f"❌ 오류가 발생했습니다: {e}", ephemeral=True)
                except Exception:
                    pass


        @bot.tree.command(name="초대순위", description="서버의 초대 순위(리더보드)를 확인합니다.")
        @app_commands.default_permissions(administrator=True)
        async def show_leaderboard(interaction: discord.Interaction):
            if interaction.guild is None:
                await interaction.response.send_message("❌ 서버 내에서만 사용 가능한 명령어입니다.", ephemeral=True)
                return

            # 관리자 권한 확인
            if not interaction.user.guild_permissions.administrator:
                await interaction.response.send_message("❌ 이 명령어는 관리자만 사용할 수 있습니다.", ephemeral=True)
                return

            await interaction.response.defer()

            # 페이징 뷰 생성
            view = LeaderboardView(bot, interaction.guild, interaction.user.id, limit=10)
            embed, has_next = await view.generate_embed()
            
            # 최초 상태 설정
            view.prev_button.disabled = True
            view.next_button.disabled = not has_next

            if not has_next and view.prev_button.disabled:
                # 1페이지만 존재하여 페이징 버튼이 전부 불필요한 경우
                await interaction.followup.send(embed=embed)
            else:
                await interaction.followup.send(embed=embed, view=view)
            
        bot.run(DISCORD_TOKEN)
