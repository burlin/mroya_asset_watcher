"""
Mroya Asset Watcher - monitors assets for updates.

Features:
- Subscribe to watch assets from DCC (Houdini/Maya)
- Listen for new AssetVersions on watched assets
- Track component availability on locations
- Auto-trigger transfers when new versions appear
- Notify DCC when updates are available
- UI tab in ftrack Connect for managing watchlist
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
import socket
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ftrack_api

# Lazy import - Connect UI may not be available at hook load time
QtWidgets = None
QtCore = None
QtGui = None

def _ensure_qt():
    """Lazy import Qt modules."""
    global QtWidgets, QtCore, QtGui
    if QtWidgets is None:
        from ftrack_connect.qt import QtWidgets as _QtWidgets, QtCore as _QtCore, QtGui as _QtGui
        QtWidgets = _QtWidgets
        QtCore = _QtCore
        QtGui = _QtGui

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bootstrap sys.path
# ---------------------------------------------------------------------------
_THIS_DIR = Path(__file__).resolve().parent
_PLUGINS_ROOT = _THIS_DIR.parent.parent
if str(_PLUGINS_ROOT) not in sys.path:
    sys.path.insert(0, str(_PLUGINS_ROOT))

# Import CachePreloader from common module
try:
    _FTRACK_INOUT_PATH = _PLUGINS_ROOT / "ftrack_inout"
    if str(_FTRACK_INOUT_PATH) not in sys.path and _FTRACK_INOUT_PATH.exists():
        sys.path.insert(0, str(_FTRACK_INOUT_PATH))
    from ftrack_inout.common.cache_preloader import CachePreloader
    CACHE_PRELOADER_AVAILABLE = True
except ImportError as e:
    logger.warning("CachePreloader not available: %s", e)
    CachePreloader = None
    CACHE_PRELOADER_AVAILABLE = False

try:
    from ftrack_inout.common.path_from_project import get_asset_display_path
    PATH_FROM_PROJECT_AVAILABLE = True
except ImportError:
    get_asset_display_path = None
    PATH_FROM_PROJECT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Watchlist Storage
# ---------------------------------------------------------------------------
class UpdateAction:
    """Actions to take when update is detected."""
    NOTIFY_ONLY = 'notify_only'           # Just show notification
    WAIT_LOCATION = 'wait_location'       # Wait for component on accessible location
    AUTO_TRANSFER = 'auto_transfer'       # Automatically trigger transfer
    AUTO_UPDATE_DCC = 'auto_update_dcc'   # Auto-transfer + update in DCC


class WatchlistStorage:
    """Persistent storage for watched assets."""
    
    def __init__(self):
        self._storage_path = self._get_storage_path()
        self._data: Dict[str, Any] = {
            'watched_assets': [],
            'settings': {
                'default_action': UpdateAction.WAIT_LOCATION,
                'notify_dcc': True,
                'check_interval_seconds': 60,
                'accessible_locations': [],  # User's accessible location IDs
            }
        }
        self._load()
    
    def _get_storage_path(self) -> Path:
        """Get path to watchlist storage file."""
        # Use ftrack config directory
        ftrack_dir = Path.home() / '.ftrack'
        ftrack_dir.mkdir(exist_ok=True)
        return ftrack_dir / 'mroya_asset_watcher.json'
    
    def _load(self):
        """Load watchlist from disk."""
        if self._storage_path.exists():
            try:
                with open(self._storage_path, 'r', encoding='utf-8') as f:
                    self._data = json.load(f)
                logger.info(f"Loaded watchlist: {len(self._data.get('watched_assets', []))} assets")
            except Exception as e:
                logger.warning(f"Failed to load watchlist: {e}")
    
    def _save(self):
        """Save watchlist to disk."""
        try:
            with open(self._storage_path, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=2, default=str)
        except Exception as e:
            logger.warning(f"Failed to save watchlist: {e}")
    
    @property
    def watched_assets(self) -> List[Dict[str, Any]]:
        return self._data.get('watched_assets', [])
    
    @property
    def settings(self) -> Dict[str, Any]:
        return self._data.get('settings', {})
    
    def add_watch(self, watch_entry: Dict[str, Any]) -> bool:
        """Add asset to watchlist."""
        # Check if already watching
        asset_id = watch_entry.get('asset_id')
        component_name = watch_entry.get('component_name')
        
        for existing in self._data['watched_assets']:
            if existing['asset_id'] == asset_id and existing.get('component_name') == component_name:
                # Update existing entry
                existing.update(watch_entry)
                existing['updated_at'] = datetime.now().isoformat()
                self._save()
                return False  # Was update, not new
        
        # Add new entry
        watch_entry['created_at'] = datetime.now().isoformat()
        watch_entry['status'] = 'watching'
        watch_entry['last_checked'] = None
        watch_entry['pending_update'] = None
        self._data['watched_assets'].append(watch_entry)
        self._save()
        return True  # Was new
    
    def remove_watch(self, asset_id: str, component_name: str = None):
        """Remove asset from watchlist."""
        self._data['watched_assets'] = [
            w for w in self._data['watched_assets']
            if not (w['asset_id'] == asset_id and 
                   (component_name is None or w.get('component_name') == component_name))
        ]
        self._save()
    
    def update_watch(self, asset_id: str, component_name: str, updates: Dict[str, Any]):
        """Update watch entry."""
        aid = str(asset_id) if asset_id is not None else ''
        cname = str(component_name) if component_name is not None else ''
        for watch in self._data['watched_assets']:
            w_aid = str(watch['asset_id']) if watch.get('asset_id') is not None else ''
            w_cname = str(watch.get('component_name') or '') or ''
            if w_aid == aid and w_cname == cname:
                watch.update(updates)
                self._save()
                return
    
    def get_watch(self, asset_id: str, component_name: str = None) -> Optional[Dict[str, Any]]:
        """Get watch entry."""
        for watch in self._data['watched_assets']:
            if watch['asset_id'] == asset_id:
                if component_name is None or watch.get('component_name') == component_name:
                    return watch
        return None
    
    def clear_all(self):
        """Clear all watches."""
        self._data['watched_assets'] = []
        self._save()


# ---------------------------------------------------------------------------
# Background Watcher
# ---------------------------------------------------------------------------
class AssetWatcherManager:
    """Background manager that listens for asset updates."""
    
    def __init__(self, session: ftrack_api.Session):
        self._session = session
        self._storage = WatchlistStorage()
        self._running = False
        self._event_thread: Optional[threading.Thread] = None
        self._poll_thread: Optional[threading.Thread] = None
        
        # Cache preloader for asset data preloading
        self._preloader = None
        if CACHE_PRELOADER_AVAILABLE and CachePreloader:
            try:
                self._preloader = CachePreloader(session)
                logger.info("CachePreloader initialized for Asset Watcher")
            except Exception as e:
                logger.warning(f"Failed to initialize CachePreloader: {e}")
        
        # Callbacks for UI updates
        self._on_update_callbacks: List[callable] = []
    
    @property
    def storage(self) -> WatchlistStorage:
        return self._storage
    
    def add_update_callback(self, callback: callable):
        """Add callback for UI updates."""
        self._on_update_callbacks.append(callback)
    
    def _notify_update(self, event_type: str, data: Dict[str, Any]):
        """Notify all callbacks about update."""
        for callback in self._on_update_callbacks:
            try:
                callback(event_type, data)
            except Exception as e:
                logger.warning(f"Callback error: {e}")
    
    def register(self):
        """Register event listeners and start watching."""
        logger.info("AssetWatcherManager: registering event listeners...")
        
        try:
            self._session.event_hub.connect()
        except Exception as e:
            logger.warning(f"event_hub.connect() failed: {e}")
        
        # Subscribe to ftrack.update for new AssetVersions
        self._session.event_hub.subscribe(
            'topic=ftrack.update',
            self._on_ftrack_update,
            priority=50
        )
        
        # Subscribe to component-added for location tracking
        self._session.event_hub.subscribe(
            'topic=ftrack.location.component-added',
            self._on_component_added,
            priority=50
        )
        
        # Subscribe to watch requests from DCC
        current_user = self._session.api_user
        self._session.event_hub.subscribe(
            f'topic=mroya.asset.watch and source.user.username="{current_user}"',
            self._on_watch_request,
            priority=10
        )
        
        # Subscribe to unwatch requests
        self._session.event_hub.subscribe(
            f'topic=mroya.asset.unwatch and source.user.username="{current_user}"',
            self._on_unwatch_request,
            priority=10
        )
        
        # Subscribe to update-accepted events
        self._session.event_hub.subscribe(
            f'topic=mroya.asset.update-accepted and source.user.username="{current_user}"',
            self._on_update_accepted,
            priority=10
        )
        
        self._running = True
        
        # Start event processing thread
        self._event_thread = threading.Thread(target=self._event_loop, daemon=True)
        self._event_thread.start()
        
        # Check for missed updates on startup (lazy, in background)
        threading.Thread(target=self._check_missed_updates, daemon=True).start()

        # Periodic polling fallback (in case ftrack.update events are not received)
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()
        
        logger.info("AssetWatcherManager: registered and running")
    
    def unregister(self):
        """Stop watching."""
        logger.info("AssetWatcherManager: unregistering...")
        self._running = False
    
    def _event_loop(self):
        """Event processing loop."""
        logger.info("[AssetWatcher] Event loop started")
        logger.info(f"[AssetWatcher] Event hub connected: {self._session.event_hub.connected}")
        
        while self._running:
            try:
                self._session.event_hub.wait(1)
            except Exception as e:
                logger.warning(f"[AssetWatcher] Event loop error: {e}")
                time.sleep(1)
        
        logger.info("[AssetWatcher] Event loop stopped")
    
    def _on_ftrack_update(self, event):
        """Handle ftrack.update events."""
        try:
            entities = event.get('data', {}).get('entities', [])

            logger.info(f"[AssetWatcher] ftrack.update received: {len(entities)} entities")

            for entity in entities:
                entity_type = (entity.get('entityType') or '').lower()
                action = (entity.get('action') or '').lower()
                changes = entity.get('changes', {})

                logger.info(f"[AssetWatcher] Entity: type={entity_type}, action={action}, id={entity.get('entityId')}, changes={list(changes.keys())}")

                # New AssetVersion created (some installations report action=add)
                if entity_type == 'assetversion' and action in ('create', 'add'):
                    logger.info(f"[AssetWatcher] ✓ AssetVersion event ({action}): {entity.get('entityId')}")
                    self._handle_new_version(entity)

                # AssetVersion status changed
                elif entity_type == 'assetversion' and action == 'update':
                    if 'statusid' in changes:
                        logger.info(f"[AssetWatcher] ✓ AssetVersion status change: {entity.get('entityId')}")
                        self._handle_status_change(entity)

                # Asset status changed
                elif entity_type == 'asset' and action == 'update':
                    if 'statusid' in changes:
                        logger.info(f"[AssetWatcher] ✓ Asset status change: {entity.get('entityId')}")
                        self._handle_status_change(entity)

        except Exception as e:
            logger.error(f"[AssetWatcher] Error handling ftrack.update: {e}", exc_info=True)

    def _handle_status_change(self, entity: Dict[str, Any]):
        """Handle AssetVersion/Asset status change."""
        _TARGET_STATUS = 'Use This'
        _USE_THIS_KEY = 'use_this_list'
        # Keys in asset.metadata that are not component entries
        _NON_COMPONENT_KEYS = {_USE_THIS_KEY, 'status_note'}

        version_id = entity.get('entityId')
        changes = entity.get('changes', {})
        new_status_id = (changes.get('statusid') or {}).get('new')
        old_status_id = (changes.get('statusid') or {}).get('old')

        logger.info(f"[AssetWatcher] _handle_status_change: version_id={version_id}, {old_status_id} -> {new_status_id}")

        try:
            entity_type = (entity.get('entityType') or '').lower()

            if entity_type == 'assetversion':
                version = self._session.query(
                    f'select id, version, status.name, asset_id, asset.name, asset.metadata, '
                    f'components.id, components.name, components.file_type '
                    f'from AssetVersion where id is "{version_id}"'
                ).first()
                if not version:
                    logger.warning(f"[AssetWatcher] AssetVersion {version_id} not found")
                    return
                asset = version['asset']
                asset_name = asset['name']
                new_status_name = version['status']['name'] if version.get('status') else 'Unknown'
            else:
                # entity_type == 'asset'
                asset = self._session.query(
                    f'select id, name, status.name, metadata '
                    f'from Asset where id is "{version_id}"'
                ).first()
                if not asset:
                    logger.warning(f"[AssetWatcher] Asset {version_id} not found")
                    return
                asset_name = asset['name']
                new_status_name = asset['status']['name'] if asset.get('status') else 'Unknown'

            logger.info(f"[AssetWatcher] Status change: '{asset_name}' -> '{new_status_name}'")

            self._notify_update('asset_status_changed', {
                'entity_id': version_id,
                'entity_type': entity_type,
                'asset_name': asset_name,
                'new_status_id': new_status_id,
                'old_status_id': old_status_id,
                'new_status_name': new_status_name,
            })

            if new_status_name != _TARGET_STATUS:
                return

            # --- Build / update use_this_list ---
            asset_meta = dict(asset.get('metadata') or {})

            # Parse existing use_this_list (or start fresh)
            try:
                use_this = json.loads(asset_meta.get(_USE_THIS_KEY) or '{}')
            except Exception:
                use_this = {}

            # Clean up any status_note that leaked into use_this_list from a previous run
            use_this.pop('status_note', None)

            # Build component dict from the specific version that was set to "Use This"
            # (NOT from asset.metadata which reflects the latest published version)
            current_components = {}
            if entity_type == 'assetversion':
                for comp in (version.get('components') or []):
                    comp_name = comp.get('name', '') or ''
                    raw_ext = comp.get('file_type', '') or ''
                    ext = str(raw_ext).lstrip('.')
                    key = f"{comp_name}.{ext}" if (comp_name and ext) else comp_name or ''
                    if key:
                        current_components[key] = comp['id']

            if not current_components:
                logger.info(f"[AssetWatcher] No components found on version for '{asset_name}', nothing to merge")
                return

            # Merge: update components that are present now, keep ones that are absent
            updated = []
            added = []
            for comp_key, comp_id in current_components.items():
                if comp_key in use_this:
                    if use_this[comp_key] != comp_id:
                        use_this[comp_key] = comp_id
                        updated.append(comp_key)
                else:
                    use_this[comp_key] = comp_id
                    added.append(comp_key)

            kept = [k for k in use_this if k not in current_components]
            logger.info(
                f"[AssetWatcher] use_this_list for '{asset_name}': "
                f"added={added}, updated={updated}, kept={kept}"
            )

            # Always remove status_note regardless of whether use_this_list changed
            had_status_note = 'status_note' in asset_meta
            asset_meta.pop('status_note', None)

            if not added and not updated and not had_status_note:
                logger.info(f"[AssetWatcher] Nothing to update for '{asset_name}', skipping commit")
                return

            asset_meta[_USE_THIS_KEY] = json.dumps(use_this)
            asset['metadata'] = asset_meta
            self._session.commit()
            logger.info(f"[AssetWatcher] ✓ use_this_list written to asset '{asset_name}' (id={asset['id']}): {use_this}")

            # --- Reset all other versions of this asset to "Published" ---
            if entity_type == 'assetversion':
                asset_id = version['asset_id']
                published_status = self._session.query(
                    'Status where name is "Published"'
                ).first()

                if not published_status:
                    logger.warning(f"[AssetWatcher] 'Published' status not found, cannot reset other versions")
                else:
                    other_versions = self._session.query(
                        f'select id, version, status.name '
                        f'from AssetVersion where asset_id is "{asset_id}" and id is_not "{version_id}"'
                    ).all()

                    to_reset = [v for v in other_versions if v['status']['name'] != 'Published']
                    for v in to_reset:
                        v['status'] = published_status

                    if to_reset:
                        self._session.commit()
                        logger.info(
                            f"[AssetWatcher] ✓ Reset {len(to_reset)} other version(s) of '{asset_name}' to 'Published': "
                            f"{[v['version'] for v in to_reset]}"
                        )
                    else:
                        logger.info(f"[AssetWatcher] All other versions of '{asset_name}' already 'Published'")

        except Exception as e:
            logger.error(f"[AssetWatcher] Error handling status change: {e}", exc_info=True)

    def _poll_loop(self):
        """Periodic polling loop as a fallback to event-based updates."""
        logger.info("[AssetWatcher] Polling loop started")
        # Small delay so Connect can finish bootstrapping.
        time.sleep(3)

        while self._running:
            try:
                self._poll_once()
            except Exception as e:
                logger.warning(f"[AssetWatcher] Polling error: {e}")

            interval = int(self._storage.settings.get('check_interval_seconds', 60) or 60)
            time.sleep(max(10, interval))

        logger.info("[AssetWatcher] Polling loop stopped")

    def _poll_once(self):
        """Check watched assets for new versions."""
        watched = list(self._storage.watched_assets or [])
        if not watched:
            logger.debug("[AssetWatcher] Poll: No watched assets")
            return

        logger.debug(f"[AssetWatcher] Poll: Checking {len(watched)} watched assets")

        # Poll unique assets.
        asset_ids = sorted({w.get('asset_id') for w in watched if w.get('asset_id')})
        logger.debug(f"[AssetWatcher] Poll: Checking {len(asset_ids)} unique assets")
        
        for asset_id in asset_ids:
            latest = self._session.query(
                f'select id, version from AssetVersion '
                f'where asset_id is "{asset_id}" '
                f'order by version descending'
            ).first()

            if not latest:
                logger.debug(f"[AssetWatcher] Poll: No versions found for asset_id={asset_id}")
                continue

            latest_id = latest['id']
            latest_version = latest.get('version')

            # If any watch for this asset is behind and doesn't already point to this pending version, process it.
            for watch in watched:
                if watch.get('asset_id') != asset_id:
                    continue

                current_version_id = watch.get('current_version_id')
                pending = watch.get('pending_update') or {}
                pending_version_id = pending.get('version_id')
                asset_name = watch.get('asset_name', 'Unknown')

                logger.debug(
                    f"[AssetWatcher] Poll: {asset_name} - "
                    f"current={current_version_id}, latest={latest_id} (v{latest_version}), "
                    f"pending={pending_version_id}"
                )

                if current_version_id and latest_id != current_version_id and pending_version_id != latest_id:
                    logger.info(
                        f"[AssetWatcher] ✓ Poll detected new version for {asset_name}: "
                        f"current={current_version_id}, latest={latest_id} (v{latest_version})"
                    )
                    # Reuse existing processing logic.
                    self._handle_new_version({'entityId': latest_id})
                    break
                elif current_version_id == latest_id:
                    logger.debug(f"[AssetWatcher] Poll: {asset_name} is up to date (v{latest_version})")
                elif pending_version_id == latest_id:
                    logger.debug(f"[AssetWatcher] Poll: {asset_name} already has pending update (v{latest_version})")

            # Update last_checked timestamps
            now_iso = datetime.now().isoformat()
            for watch in self._storage.watched_assets:
                if watch.get('asset_id') == asset_id:
                    watch['last_checked'] = now_iso
            self._storage._save()
    
    def _handle_new_version(self, entity: Dict[str, Any]):
        """Handle new AssetVersion creation."""
        version_id = entity.get('entityId')
        
        logger.info(f"[AssetWatcher] _handle_new_version called for version_id: {version_id}")
        
        # Get version details
        try:
            version = self._session.query(
                f'select id, version, asset_id, asset.name, asset.parent.name, '
                f'components.id, components.name, components.component_locations.location_id '
                f'from AssetVersion where id is "{version_id}"'
            ).first()
            
            if not version:
                logger.warning(f"[AssetWatcher] Version {version_id} not found in ftrack")
                return
            
            asset_id = version['asset_id']
            asset_name = version['asset']['name']
            version_number = version['version']
            
            logger.info(f"[AssetWatcher] Processing version: {asset_name} v{version_number} (asset_id={asset_id})")
            
            # Check if we're watching this asset
            watched_count = len(self._storage.watched_assets)
            logger.info(f"[AssetWatcher] Checking {watched_count} watched assets...")
            
            for watch in self._storage.watched_assets:
                if watch['asset_id'] != asset_id:
                    continue
                
                logger.info(f"[AssetWatcher] ✓ Found watch for asset: {asset_name}")
                logger.info(f"[AssetWatcher]   Watch component_name: {watch.get('component_name')}")
                logger.info(f"[AssetWatcher]   Watch current_version_id: {watch.get('current_version_id')}")
                
                # Check if component matches
                component_name = watch.get('component_name')
                matching_component = None
                
                all_components = version.get('components', [])
                logger.info(f"[AssetWatcher]   Version has {len(all_components)} components: {[c.get('name') for c in all_components]}")
                
                if component_name:
                    for comp in all_components:
                        if comp['name'] == component_name:
                            matching_component = comp
                            logger.info(f"[AssetWatcher]   ✓ Found matching component: {component_name} (id={comp.get('id')})")
                            break
                    if not matching_component:
                        logger.info(f"[AssetWatcher]   ✗ Component '{component_name}' not found in new version - skipping")
                        continue
                else:
                    # If no specific component, use first one
                    if all_components:
                        matching_component = all_components[0]
                        logger.info(f"[AssetWatcher]   Using first component (no specific name): {matching_component.get('name')}")
                    else:
                        logger.warning(f"[AssetWatcher]   No components in version - skipping")
                        continue
                
                # ----- From here down: runs for BOTH cases (with or without explicit component_name) -----
                # Determine action
                action = watch.get('update_action') or self._storage.settings.get('default_action', UpdateAction.WAIT_LOCATION)
                
                # Check if component is already on accessible location
                accessible_locations = self._storage.settings.get('accessible_locations', [])
                target_location = watch.get('target_location_id')
                available_location = None
                
                if matching_component:
                    comp_locations = matching_component.get('component_locations', [])
                    for cl in comp_locations:
                        loc_id = cl.get('location_id')
                        if loc_id == target_location:
                            available_location = loc_id
                            break
                        elif loc_id in accessible_locations:
                            available_location = loc_id
                    
                    # Determine initial status based on availability and action
                    logger.info(f"[AssetWatcher]   Component locations: {[cl.get('location_id') for cl in matching_component.get('component_locations', [])]}")
                    logger.info(f"[AssetWatcher]   Target location: {target_location}")
                    logger.info(f"[AssetWatcher]   Available location: {available_location}")
                    logger.info(f"[AssetWatcher]   Update action: {action}")
                    
                    if available_location == target_location:
                        # Already on target - ready to update
                        initial_status = 'ready_to_update'
                        pending_status = 'available'
                    elif available_location:
                        # On accessible location but not target
                        if action in (UpdateAction.AUTO_TRANSFER, UpdateAction.AUTO_UPDATE_DCC):
                            initial_status = 'transferring'
                            pending_status = 'transferring'
                        else:
                            initial_status = 'ready_to_update'  # Can manually transfer
                            pending_status = 'available'
                    else:
                        # Not on any accessible location - wait
                        if action == UpdateAction.NOTIFY_ONLY:
                            initial_status = 'update_available'
                            pending_status = 'new_version'
                        else:
                            initial_status = 'waiting_location'
                            pending_status = 'waiting_location'
                    
                    logger.info(f"[AssetWatcher]   Determined status: {initial_status}")
                    
                    # Update watch entry
                    self._storage.update_watch(
                        asset_id,
                        watch.get('component_name'),
                        {
                            'pending_update': {
                                'version_id': version_id,
                                'version_number': version['version'],
                                'discovered_at': datetime.now().isoformat(),
                                'status': pending_status,
                                'component_id': matching_component['id'] if matching_component else None,
                                'available_location_id': available_location,
                            },
                            'status': initial_status,
                        }
                    )
                    
                    logger.info(f"[AssetWatcher]   ✓ Watch entry updated")
                    
                    # Notify UI
                    self._notify_update('new_version', {
                        'asset_id': asset_id,
                        'asset_name': asset_name,
                        'version_id': version_id,
                        'version_number': version['version'],
                        'component_name': component_name,
                        'status': initial_status,
                        'action': action,
                    })
                    
                    # Notify DCC if enabled
                    notify_enabled = watch.get('notify_dcc', True) and self._storage.settings.get('notify_dcc', True)
                    logger.info(f"[AssetWatcher]   Notify DCC enabled: {notify_enabled}")
                    
                    if notify_enabled:
                        logger.info(f"[AssetWatcher]   → Calling _notify_dcc for {asset_name}/{component_name}")
                        self._notify_dcc(watch, {
                            'asset_id': asset_id,
                            'asset_name': asset_name,
                            'component_name': component_name,
                            'version_number': version['version'],
                            'status': initial_status,
                            'component_id': matching_component['id'] if matching_component else None,
                            'version_id': version_id,
                        })
                    else:
                        logger.info(f"[AssetWatcher]   ✗ DCC notification skipped (disabled)")
                    
                    # Trigger transfer if auto and available on accessible location
                    if initial_status == 'transferring' and available_location and available_location != target_location:
                        pending = {
                            'version_id': version_id,
                            'component_id': matching_component['id'] if matching_component else None,
                        }
                        self._trigger_transfer_from_location(watch, pending, available_location)
                    
        except Exception as e:
            logger.error(f"Error handling new version: {e}")
    
    def _on_component_added(self, event):
        """Handle component added to location."""
        try:
            component_id = event.get('data', {}).get('componentId')
            location_id = event.get('data', {}).get('locationId')
            
            if not component_id or not location_id:
                return
            
            # Get accessible locations from settings
            accessible_locations = self._storage.settings.get('accessible_locations', [])
            target_location = None
            
            # Check if any watched asset is waiting for this component
            for watch in self._storage.watched_assets:
                status = watch.get('status')
                pending = watch.get('pending_update', {})
                target_loc = watch.get('target_location_id')
                
                if not pending:
                    continue
                
                # Check if this is the component we're waiting for
                pending_component_id = pending.get('component_id')
                
                # Case 1: Waiting for component to appear on ANY accessible location
                if status == 'waiting_location':
                    if location_id in accessible_locations or location_id == target_loc:
                        # Check if this component belongs to our watched version
                        if self._is_component_for_watch(component_id, watch, pending):
                            logger.info(
                                f"Component appeared on accessible location: "
                                f"{watch.get('asset_name')}/{watch.get('component_name')} "
                                f"on location {location_id}"
                            )
                            
                            self._storage.update_watch(
                                watch['asset_id'],
                                watch.get('component_name'),
                                {
                                    'pending_update': {
                                        **pending,
                                        'status': 'available',
                                        'available_location_id': location_id,
                                    },
                                    'status': 'ready_to_update',
                                }
                            )
                            
                            self._notify_update('component_available', {
                                'asset_id': watch['asset_id'],
                                'asset_name': watch.get('asset_name'),
                                'component_name': watch.get('component_name'),
                                'location_id': location_id,
                            })
                            
                            # Preload asset data after component appears on accessible location
                            self._preload_asset_data(watch['asset_id'])
                            
                            # Check if should auto-transfer to target
                            action = watch.get('update_action') or self._storage.settings.get('default_action')
                            if action in (UpdateAction.AUTO_TRANSFER, UpdateAction.AUTO_UPDATE_DCC):
                                if location_id != target_loc:
                                    self._trigger_transfer_from_location(watch, pending, location_id)
                
                # Case 2: Transfer in progress, component arrived at target
                elif status == 'transferring' and location_id == target_loc:
                    if self._is_component_for_watch(component_id, watch, pending):
                        logger.info(f"Transfer complete: {watch.get('asset_name')}/{watch.get('component_name')}")
                        
                        self._storage.update_watch(
                            watch['asset_id'],
                            watch.get('component_name'),
                            {
                                'pending_update': {
                                    **pending,
                                    'status': 'transferred',
                                },
                                'status': 'ready_to_update',
                            }
                        )
                        
                        self._notify_update('transfer_complete', {
                            'asset_id': watch['asset_id'],
                            'asset_name': watch.get('asset_name'),
                            'component_name': watch.get('component_name'),
                        })
                        
                        # Preload asset data after component appears on target location
                        self._preload_asset_data(watch['asset_id'])
                    
        except Exception as e:
            logger.error(f"Error handling component-added: {e}")
    
    def _is_component_for_watch(self, component_id: str, watch: Dict, pending: Dict) -> bool:
        """Check if component belongs to watched asset/version."""
        try:
            # Query component details
            component = self._session.query(
                f'select id, name, version.id, version.asset_id '
                f'from Component where id is "{component_id}"'
            ).first()
            
            if not component:
                return False
            
            # Check asset match
            if component['version']['asset_id'] != watch['asset_id']:
                return False
            
            # Check version match (if we know which version we're waiting for)
            pending_version_id = pending.get('version_id')
            if pending_version_id and component['version']['id'] != pending_version_id:
                return False
            
            # Check component name match
            watch_component_name = watch.get('component_name')
            if watch_component_name and component['name'] != watch_component_name:
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Error checking component match: {e}")
            return False
    
    def _notify_dcc(self, watch: Dict, update_data: Dict):
        """Send notification to DCC about update."""
        try:
            source_dcc = watch.get('source_dcc', 'unknown')
            
            # Publish event that DCC listeners can receive
            current_hostname = socket.gethostname().lower()
            
            event_data = {
                'asset_id': update_data.get('asset_id'),
                'asset_name': update_data.get('asset_name'),
                'component_name': update_data.get('component_name'),
                'version_number': update_data.get('version_number'),
                'version_id': update_data.get('version_id'),
                'component_id': update_data.get('component_id'),
                'status': update_data.get('status'),
                'source_dcc': source_dcc,
                'scene_path': watch.get('scene_path'),
            }
            
            logger.info(f"[AssetWatcher] _notify_dcc: Publishing mroya.asset.update-notify")
            logger.info(f"[AssetWatcher]   Topic: mroya.asset.update-notify")
            logger.info(f"[AssetWatcher]   Data: {event_data}")
            logger.info(f"[AssetWatcher]   Source DCC: {source_dcc}")
            logger.info(f"[AssetWatcher]   Hostname: {current_hostname}")
            
            self._session.event_hub.publish(
                ftrack_api.event.base.Event(
                    topic='mroya.asset.update-notify',
                    data=event_data,
                    source={'hostname': current_hostname}
                ),
                on_error='ignore'
            )
            
            logger.info(f"[AssetWatcher] ✓ DCC notification published: {update_data.get('asset_name')}/{update_data.get('component_name')} v{update_data.get('version_number')} [{update_data.get('status')}]")
            
        except Exception as e:
            logger.error(f"[AssetWatcher] ✗ Failed to notify DCC: {e}", exc_info=True)
    
    def _trigger_transfer_from_location(self, watch: Dict, pending: Dict, source_location_id: str):
        """Trigger transfer from source location to target."""
        target_location = watch.get('target_location_id')
        component_name = watch.get('component_name')
        
        if not target_location:
            return
        
        try:
            # Find component ID for this version
            version_id = pending.get('version_id')
            if not version_id:
                return
            
            component = self._session.query(
                f'select id from Component where version_id is "{version_id}" '
                f'and name is "{component_name}"'
            ).first()
            
            if not component:
                return
            
            current_hostname = socket.gethostname().lower()
            
            self._session.event_hub.publish(
                ftrack_api.event.base.Event(
                    topic='mroya.transfer.request',
                    data={
                        'component_ids': [component['id']],
                        'source_location_id': source_location_id,
                        'target_location_id': target_location,
                        'auto_triggered': True,
                        'source': 'asset_watcher',
                    },
                    source={'hostname': current_hostname}
                ),
                on_error='ignore'
            )
            
            self._storage.update_watch(
                watch['asset_id'],
                component_name,
                {
                    'pending_update': {
                        **pending,
                        'status': 'transferring',
                        'component_id': component['id'],
                    },
                    'status': 'transferring',
                }
            )
            
            logger.info(f"Transfer triggered: {watch['asset_name']}/{component_name} from {source_location_id}")
            
        except Exception as e:
            logger.error(f"Error triggering transfer: {e}")
    
    def trigger_transfer_for_watch(self, watch: Dict[str, Any]) -> Tuple[bool, str]:
        """Manually trigger transfer for a watched asset (called from UI).
        
        Returns:
            (success, message) - success True if transfer was triggered, message for user.
        """
        pending = (watch.get('pending_update') or {})
        version_id = pending.get('version_id')
        component_id = pending.get('component_id')
        target_location = watch.get('target_location_id')
        component_name = watch.get('component_name')
        asset_name = watch.get('asset_name', 'Unknown')
        
        if not target_location:
            return False, "No target location set for this watch."
        if not pending or not version_id:
            return False, "No pending update to transfer (asset is up to date or no new version yet)."
        
        # If component is already on an accessible location, transfer from there
        source_location_id = pending.get('available_location_id')
        if source_location_id:
            self._trigger_transfer_from_location(watch, pending, source_location_id)
            self._notify_update('transfer_triggered', {'watch': watch})
            return True, f"Transfer started: {asset_name} from {source_location_id} → {target_location}"
        
        # Published but not on any accessible location: request transfer (server/auto-detect source)
        if not component_id and component_name:
            comp = self._session.query(
                f'select id from Component where version_id is "{version_id}" and name is "{component_name}"'
            ).first()
            if comp:
                component_id = comp['id']
        if not component_id:
            return False, "Could not resolve component for this version."
        
        try:
            current_hostname = socket.gethostname().lower()
            self._session.event_hub.publish(
                ftrack_api.event.base.Event(
                    topic='mroya.transfer.request',
                    data={
                        'component_ids': [component_id],
                        'source_location_id': None,
                        'target_location_id': target_location,
                        'auto_triggered': False,
                        'source': 'asset_watcher_ui',
                    },
                    source={'hostname': current_hostname}
                ),
                on_error='ignore'
            )
            self._storage.update_watch(
                watch['asset_id'],
                component_name,
                {
                    'pending_update': {**pending, 'status': 'transferring', 'component_id': component_id},
                    'status': 'transferring',
                }
            )
            self._notify_update('transfer_triggered', {'watch': watch})
            logger.info(f"Transfer requested (auto-detect source): {asset_name} → {target_location}")
            return True, f"Transfer requested for {asset_name} (source will be resolved by transfer manager)."
        except Exception as e:
            logger.error(f"Error requesting transfer: {e}")
            return False, str(e)
    
    def _preload_asset_data(self, asset_id: str):
        """Preload asset data after component appears on accessible location.
        
        This is called after a component has been downloaded and registered
        on the target location. It preloads asset, versions, and components
        into the shared cache for fast access in Houdini/Maya.
        """
        if not self._preloader:
            logger.debug("CachePreloader not available - skipping preload")
            return
        
        try:
            logger.info(f"[AssetWatcher] Preloading asset data for asset_id={asset_id}...")
            result = self._preloader.preload_asset_data(asset_id, max_versions=50)
            if result.get('success'):
                logger.info(
                    f"[AssetWatcher] ✓ Asset preloaded: {result.get('loaded_count')} entities "
                    f"in {result.get('elapsed_ms', 0):.1f}ms"
                )
            else:
                logger.warning(f"[AssetWatcher] ✗ Asset preload failed: {result.get('error')}")
        except Exception as e:
            logger.warning(f"[AssetWatcher] Failed to preload asset data: {e}")
    
    def _on_watch_request(self, event):
        """Handle watch request from DCC."""
        try:
            data = event.get('data', {})
            logger.info(f"Watch request received: {data}")
            
            # Determine update_action from data or use default
            update_action = data.get('update_action')
            if update_action is None:
                # Map old auto_transfer flag to new action
                if data.get('auto_transfer', False):
                    update_action = UpdateAction.AUTO_TRANSFER
                else:
                    update_action = self._storage.settings.get('default_action', UpdateAction.WAIT_LOCATION)
            
            # Get human-readable names for version and location
            # Prefer values from event data, fall back to querying ftrack
            current_version_number = data.get('current_version_number')
            target_location_name = data.get('target_location_name')
            
            # Fallback: query ftrack if not provided
            if not current_version_number:
                version_id = data.get('current_version_id')
                if version_id:
                    try:
                        ver = self._session.query(f'AssetVersion where id is "{version_id}"').first()
                        if ver:
                            current_version_number = ver['version']
                    except Exception as e:
                        logger.warning(f"Could not get version number: {e}")
            
            if not target_location_name:
                location_id = data.get('target_location_id')
                if location_id:
                    try:
                        loc = self._session.query(f'Location where id is "{location_id}"').first()
                        if loc:
                            target_location_name = loc.get('name') or loc.get('label')
                    except Exception as e:
                        logger.warning(f"Could not get location name: {e}")
            
            watch_entry = {
                'asset_id': data.get('asset_id'),
                'asset_name': data.get('asset_name'),
                'component_name': data.get('component_name'),
                'component_id': data.get('component_id'),
                'target_location_id': data.get('target_location_id'),
                'target_location_name': target_location_name,
                'current_version_id': data.get('current_version_id'),
                'current_version_number': current_version_number,
                'source_dcc': data.get('source_dcc'),
                'scene_path': data.get('scene_path'),
                'update_action': update_action,
                'notify_dcc': data.get('notify_dcc', True),
            }
            if PATH_FROM_PROJECT_AVAILABLE and get_asset_display_path and watch_entry.get('asset_id'):
                try:
                    watch_entry['asset_path'] = get_asset_display_path(self._session, str(watch_entry['asset_id']))
                except Exception:
                    pass
            
            is_new = self._storage.add_watch(watch_entry)
            
            self._notify_update('watch_added', {
                'watch': watch_entry,
                'is_new': is_new,
            })
            
        except Exception as e:
            logger.error(f"Error handling watch request: {e}")
    
    def _on_unwatch_request(self, event):
        """Handle unwatch request."""
        try:
            data = event.get('data', {})
            asset_id = data.get('asset_id')
            component_name = data.get('component_name')
            
            self._storage.remove_watch(asset_id, component_name)
            
            self._notify_update('watch_removed', {
                'asset_id': asset_id,
                'component_name': component_name,
            })
            
        except Exception as e:
            logger.error(f"Error handling unwatch request: {e}")
    
    def _on_update_accepted(self, event):
        """Handle update-accepted event from DCC."""
        try:
            data = event.get('data', {})
            asset_id = data.get('asset_id')
            component_name = data.get('component_name')
            version_number = data.get('version_number')
            component_id = data.get('component_id')
            # Normalize for comparison (event may send str; storage may have str from JSON)
            aid = str(asset_id) if asset_id is not None else ''
            cname = str(component_name) if component_name is not None else ''

            logger.info(f"[AssetWatcher] Update accepted: asset_id={asset_id} component_name={component_name} v{version_number} (normalized: aid={aid!r} cname={cname!r})")

            # Find watch entry
            for watch in self._storage.watched_assets:
                w_aid = str(watch.get('asset_id') or '') or ''
                w_cname = str(watch.get('component_name') or '') or ''
                if w_aid == aid and w_cname == cname:
                    # Use version_id from event, fallback to querying from component
                    event_version_id = data.get('version_id')
                    if not event_version_id and component_id:
                        try:
                            comp = self._session.get('Component', component_id)
                            if comp:
                                event_version_id = comp['version']['id']
                        except Exception as e:
                            logger.warning(f"Could not get version_id from component: {e}")
                    
                    # Update watch entry: clear pending_update, update current_version_id, reset status
                    update_data = {
                        'current_version_id': event_version_id,
                        'current_version_number': version_number,
                        'pending_update': None,
                        'status': 'watching',
                    }
                    
                    self._storage.update_watch(asset_id, component_name, update_data)
                    
                    logger.info(f"[AssetWatcher] ✓ Watch entry updated: now watching v{version_number}")
                    
                    # Notify UI
                    self._notify_update('update_accepted', {
                        'asset_id': asset_id,
                        'asset_name': watch.get('asset_name'),
                        'component_name': component_name,
                        'version_number': version_number,
                    })
                    
                    break
            else:
                # No matching watch entry
                watched = [(str(w.get('asset_id')), str(w.get('component_name', ''))) for w in self._storage.watched_assets]
                logger.warning(
                    "[AssetWatcher] update-accepted: no matching watch entry for asset_id=%s component_name=%s. "
                    "Current watches: %s",
                    aid, cname, watched,
                )
                    
        except Exception as e:
            logger.error(f"[AssetWatcher] Error handling update-accepted: {e}", exc_info=True)
    
    
    def _check_missed_updates(self):
        """Check for updates that happened while offline."""
        logger.info("Checking for missed updates...")
        time.sleep(5)  # Wait for session to be ready
        
        try:
            for watch in self._storage.watched_assets:
                current_version_id = watch.get('current_version_id')
                asset_id = watch.get('asset_id')

                if not asset_id:
                    continue

                # Query latest version
                latest = self._session.query(
                    f'SELECT id, version FROM AssetVersion '
                    f'WHERE asset_id is \"{asset_id}\" '
                    f'ORDER BY version DESC'
                ).first()

                if latest and current_version_id and latest['id'] != current_version_id:
                    logger.info(
                        f"[AssetWatcher] Missed update found for {watch.get('asset_name')}: "
                        f"current={current_version_id}, latest={latest['id']} (v{latest['version']})"
                    )

                    # Прогоняем как обычное ftrack.update-событие, чтобы сработал _handle_new_version и _notify_dcc.
                    try:
                        self._session.event_hub.publish(
                            ftrack_api.event.base.Event(
                                topic='mroya.asset.update-notify',
                                data={
                                    'asset_id': asset_id,
                                    'asset_name': watch.get('asset_name'),
                                    'component_name': watch.get('component_name'),
                                    'version_number': latest['version'],
                                    'version_id': latest['id'],
                                    'status': 'update_available',
                                    'source_dcc': watch.get('source_dcc', 'unknown'),
                                    'scene_path': watch.get('scene_path'),
                                },
                                source={'hostname': socket.gethostname().lower()},
                            ),
                            on_error='ignore',
                        )
                        logger.info(
                            f"[AssetWatcher] ✓ Published missed-update mroya.asset.update-notify for "
                            f"{watch.get('asset_name')}/{watch.get('component_name')} v{latest['version']}"
                        )
                    except Exception as inner_exc:
                        logger.error(
                            f"[AssetWatcher] Error publishing missed-update notification: {inner_exc}",
                            exc_info=True,
                        )

                # отметим время последней проверки, чтобы видеть, что _check_missed_updates действительно отработал
                watch['last_checked'] = datetime.now().isoformat()

            self._storage._save()

        except Exception as e:
            logger.error(f"[AssetWatcher] Error checking missed updates: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# UI Widget for ftrack Connect (created lazily to avoid import issues)
# ---------------------------------------------------------------------------
def create_watcher_widget(session: ftrack_api.Session, manager: AssetWatcherManager):
    """Factory function to create AssetWatcherWidget.
    
    Qt imports are done lazily to avoid issues during hook loading.
    """
    _ensure_qt()
    
    class AssetWatcherWidget(QtWidgets.QWidget):
        """Widget showing watched assets and their status."""
        
        update_signal = QtCore.Signal(str, dict)
        
        def __init__(self, session: ftrack_api.Session, parent=None):
            super().__init__(parent)
            self.session = session
            self.manager: Optional[AssetWatcherManager] = None
            
            self.update_signal.connect(self._on_update)
            
            self._setup_ui()
        
        def _setup_ui(self):
            layout = QtWidgets.QVBoxLayout(self)
            layout.setContentsMargins(10, 10, 10, 10)
            
            # Header
            header = QtWidgets.QHBoxLayout()
            
            title = QtWidgets.QLabel("Asset Watcher")
            title.setStyleSheet("font-size: 16px; font-weight: bold;")
            header.addWidget(title)
            
            header.addStretch()
            
            # Refresh button
            refresh_btn = QtWidgets.QPushButton("Refresh")
            refresh_btn.clicked.connect(self._refresh_list)
            header.addWidget(refresh_btn)
            
            # Settings button
            settings_btn = QtWidgets.QPushButton("⚙")
            settings_btn.setMaximumWidth(30)
            settings_btn.setToolTip("Settings")
            settings_btn.clicked.connect(self._show_settings)
            header.addWidget(settings_btn)
            
            # Clear all button
            clear_btn = QtWidgets.QPushButton("Clear All")
            clear_btn.clicked.connect(self._clear_all)
            header.addWidget(clear_btn)
            
            layout.addLayout(header)
            
            # Settings panel (collapsible)
            self.settings_panel = QtWidgets.QGroupBox("Settings")
            self.settings_panel.setVisible(False)
            settings_layout = QtWidgets.QVBoxLayout(self.settings_panel)
            
            # Default action selector
            action_row = QtWidgets.QHBoxLayout()
            action_row.addWidget(QtWidgets.QLabel("Default action on update:"))
            
            self.action_combo = QtWidgets.QComboBox()
            self.action_combo.addItem("Notify only", UpdateAction.NOTIFY_ONLY)
            self.action_combo.addItem("Wait for accessible location", UpdateAction.WAIT_LOCATION)
            self.action_combo.addItem("Auto-transfer to target", UpdateAction.AUTO_TRANSFER)
            self.action_combo.addItem("Auto-transfer + update DCC", UpdateAction.AUTO_UPDATE_DCC)
            self.action_combo.currentIndexChanged.connect(self._on_action_changed)
            action_row.addWidget(self.action_combo)
            action_row.addStretch()
            settings_layout.addLayout(action_row)
            
            # Notify DCC checkbox
            self.notify_dcc_cb = QtWidgets.QCheckBox("Notify DCC when update available")
            self.notify_dcc_cb.setChecked(True)
            self.notify_dcc_cb.toggled.connect(self._on_notify_changed)
            settings_layout.addWidget(self.notify_dcc_cb)
            
            # Accessible locations info
            self.locations_label = QtWidgets.QLabel("Accessible locations: loading...")
            settings_layout.addWidget(self.locations_label)
            
            layout.addWidget(self.settings_panel)
            
            # Watchlist table
            self.table = QtWidgets.QTableWidget()
            self.table.setColumnCount(7)
            self.table.setHorizontalHeaderLabels([
                "Asset", "Component", "Current", "Status", "Location", "DCC", "Actions"
            ])
            self.table.horizontalHeader().setStretchLastSection(True)
            self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
            self.table.setAlternatingRowColors(True)
            layout.addWidget(self.table)
            
            # Status bar
            self.status_label = QtWidgets.QLabel("Ready")
            self.status_label.setStyleSheet("font-size: 11px;")
            layout.addWidget(self.status_label)
        
        def set_manager(self, manager_instance: AssetWatcherManager):
            """Set the manager and register for updates."""
            self.manager = manager_instance
            self.manager.add_update_callback(self._on_manager_update)
            self._refresh_list()
        
        def _on_manager_update(self, event_type: str, data: Dict[str, Any]):
            """Called from manager (may be different thread)."""
            self.update_signal.emit(event_type, data)
        
        def _on_update(self, event_type: str, data: Dict[str, Any]):
            """Handle update in UI thread."""
            self._refresh_list()
            
            if event_type == 'new_version':
                status = data.get('status', '')
                self.status_label.setText(
                    f"New version: {data.get('asset_name')} v{data.get('version_number')} [{status}]"
                )
            elif event_type == 'watch_added':
                self.status_label.setText(
                    f"Now watching: {data.get('watch', {}).get('asset_name')}"
                )
            elif event_type == 'component_available':
                self.status_label.setText(
                    f"Available: {data.get('asset_name')}/{data.get('component_name')}"
                )
            elif event_type == 'transfer_complete':
                self.status_label.setText(
                    f"Transfer complete: {data.get('asset_name')}/{data.get('component_name')}"
                )
            elif event_type == 'update_accepted':
                self.status_label.setText(
                    f"Updated in DCC: {data.get('asset_name')} v{data.get('version_number')}"
                )
        
        def _show_settings(self):
            """Toggle settings panel visibility."""
            self.settings_panel.setVisible(not self.settings_panel.isVisible())
            self._load_settings()
        
        def _load_settings(self):
            """Load current settings into UI."""
            if not self.manager:
                return
            
            settings = self.manager.storage.settings
            
            # Set action combo
            action = settings.get('default_action', UpdateAction.WAIT_LOCATION)
            for i in range(self.action_combo.count()):
                if self.action_combo.itemData(i) == action:
                    self.action_combo.setCurrentIndex(i)
                    break
            
            # Set notify checkbox
            self.notify_dcc_cb.setChecked(settings.get('notify_dcc', True))
            
            # Show accessible locations
            locations = settings.get('accessible_locations', [])
            if locations:
                self.locations_label.setText(f"Accessible locations: {', '.join(locations)}")
            else:
                self.locations_label.setText("Accessible locations: (detecting...)")
                self._detect_accessible_locations()
        
        def _detect_accessible_locations(self):
            """Detect user's accessible locations."""
            try:
                # Query all locations and check which are accessible
                locations = self.session.query('Location').all()
                accessible = []
                
                for loc in locations:
                    # Skip unmanaged and origin locations
                    if loc['name'] in ('ftrack.unmanaged', 'ftrack.origin', 'ftrack.server'):
                        continue
                    
                    # Check if location is accessible (has accessor configured)
                    try:
                        accessor = loc.accessor
                        if accessor:
                            accessible.append(loc['id'])
                    except Exception:
                        pass
                
                if self.manager:
                    self.manager.storage._data['settings']['accessible_locations'] = accessible
                    self.manager.storage._save()
                
                # Get location names
                loc_names = [loc['name'] for loc in locations if loc['id'] in accessible]
                self.locations_label.setText(f"Accessible locations: {', '.join(loc_names) or 'none'}")
                
            except Exception as e:
                logger.warning(f"Error detecting locations: {e}")
        
        def _on_action_changed(self, index):
            """Handle action combo change."""
            if not self.manager:
                return
            
            action = self.action_combo.itemData(index)
            self.manager.storage._data['settings']['default_action'] = action
            self.manager.storage._save()
        
        def _on_notify_changed(self, checked):
            """Handle notify checkbox change."""
            if not self.manager:
                return
            
            self.manager.storage._data['settings']['notify_dcc'] = checked
            self.manager.storage._save()
        
        def _refresh_list(self):
            """Refresh the watchlist display."""
            if not self.manager:
                logger.warning("[AssetWatcher UI] _refresh_list: no manager")
                return
            
            # Reload from file to get fresh data
            self.manager.storage._load()
            
            watches = self.manager.storage.watched_assets
            logger.info(f"[AssetWatcher UI] _refresh_list: {len(watches)} watches")
            for i, w in enumerate(watches):
                logger.info(f"[AssetWatcher UI]   [{i}] {w.get('asset_name')}: loc_name={w.get('target_location_name')}, ver={w.get('current_version_number')}")
            
            self.table.setRowCount(len(watches))
            
            # Set row height to fit buttons
            self.table.verticalHeader().setDefaultSectionSize(34)
            
            for row, watch in enumerate(watches):
                # Asset: full path from project root if available, else asset_name (compute for old entries without asset_path)
                asset_display = watch.get('asset_path')
                if not asset_display and watch.get('asset_id') and PATH_FROM_PROJECT_AVAILABLE and get_asset_display_path and self.session:
                    try:
                        asset_display = get_asset_display_path(self.session, str(watch['asset_id']))
                    except Exception:
                        pass
                if not asset_display:
                    asset_display = watch.get('asset_name', 'Unknown')
                self.table.setItem(row, 0, QtWidgets.QTableWidgetItem(asset_display))
                
                # Component
                self.table.setItem(row, 1, QtWidgets.QTableWidgetItem(
                    watch.get('component_name', '*')
                ))
                
                # Current version - show version number, not UUID
                current_version = watch.get('current_version_number')
                if current_version:
                    current = f"v{current_version}"
                else:
                    # Fallback: try to get version number from ftrack
                    version_id = watch.get('current_version_id')
                    if version_id and self.session:
                        try:
                            ver = self.session.query(f'AssetVersion where id is "{version_id}"').first()
                            if ver:
                                current = f"v{ver['version']}"
                            else:
                                current = '-'
                        except Exception:
                            current = '-'
                    else:
                        current = '-'
                
                pending = watch.get('pending_update', {})
                if pending and pending.get('version_number'):
                    current = f"v{pending['version_number']} (new!)"
                self.table.setItem(row, 2, QtWidgets.QTableWidgetItem(current))
                
                # Status with color and icon
                status = watch.get('status', 'watching')
                status_display = {
                    'watching': '👁 Watching',
                    'update_available': '🆕 Update available',
                    'waiting_location': '⏳ Waiting for location',
                    'transferring': '📥 Transferring...',
                    'ready_to_update': '✅ Ready to update',
                }.get(status, status)
                
                status_item = QtWidgets.QTableWidgetItem(status_display)
                status_colors = {
                    'watching': '#3a3a4a',
                    'update_available': '#4a5a4a',
                    'waiting_location': '#5a5a3a',
                    'transferring': '#5a4a3a',
                    'ready_to_update': '#3a6a3a',
                }
                if status in status_colors:
                    status_item.setBackground(QtGui.QColor(status_colors[status]))
                self.table.setItem(row, 3, status_item)
                
                # Target location - show name, not UUID
                location_name = watch.get('target_location_name')
                if not location_name:
                    # Fallback: try to get location name from ftrack
                    location_id = watch.get('target_location_id')
                    if location_id and self.session:
                        try:
                            loc = self.session.query(f'Location where id is "{location_id}"').first()
                            if loc:
                                location_name = loc.get('name') or loc.get('label') or location_id
                            else:
                                location_name = location_id
                        except Exception:
                            location_name = location_id
                    else:
                        location_name = '-'
                self.table.setItem(row, 4, QtWidgets.QTableWidgetItem(location_name))
                
                # Source DCC
                self.table.setItem(row, 5, QtWidgets.QTableWidgetItem(
                    watch.get('source_dcc', '-')
                ))
                
                # Actions
                actions_widget = QtWidgets.QWidget()
                actions_layout = QtWidgets.QHBoxLayout(actions_widget)
                actions_layout.setContentsMargins(2, 2, 2, 2)
                actions_layout.setSpacing(2)
                
                # Transfer button
                transfer_btn = QtWidgets.QPushButton("Transfer")
                transfer_btn.setMinimumWidth(70)
                transfer_btn.clicked.connect(lambda checked=False, w=watch: self._trigger_transfer(w))
                actions_layout.addWidget(transfer_btn)
                
                # Remove button
                remove_btn = QtWidgets.QPushButton("×")
                remove_btn.setMaximumWidth(30)
                remove_btn.clicked.connect(lambda checked=False, w=watch: self._remove_watch(w))
                actions_layout.addWidget(remove_btn)
                
                self.table.setCellWidget(row, 6, actions_widget)
            
            self.table.resizeColumnsToContents()
            self.status_label.setText(f"Watching {len(watches)} assets")
        
        def _trigger_transfer(self, watch: Dict[str, Any]):
            """Manually trigger transfer for watch."""
            if not self.manager:
                return
            success, message = self.manager.trigger_transfer_for_watch(watch)
            if success:
                self._refresh_list()
            QtWidgets.QMessageBox.information(
                self, "Transfer",
                message
            )
        
        def _remove_watch(self, watch: Dict[str, Any]):
            """Remove watch from list."""
            if not self.manager:
                return
            
            reply = QtWidgets.QMessageBox.question(
                self, "Remove Watch",
                f"Stop watching {watch.get('asset_name')}?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
            )
            
            if reply == QtWidgets.QMessageBox.Yes:
                self.manager.storage.remove_watch(
                    watch.get('asset_id'),
                    watch.get('component_name')
                )
                self._refresh_list()
        
        def _clear_all(self):
            """Clear all watches."""
            if not self.manager:
                return
            
            reply = QtWidgets.QMessageBox.question(
                self, "Clear All",
                "Remove all watched assets?",
                QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No
            )
            
            if reply == QtWidgets.QMessageBox.Yes:
                self.manager.storage.clear_all()
                self._refresh_list()
    
    # Create and return widget
    widget = AssetWatcherWidget(session)
    widget.set_manager(manager)
    return widget


# ---------------------------------------------------------------------------
# Plugin Registration
# ---------------------------------------------------------------------------

# Global instances
_watcher_manager: Optional[AssetWatcherManager] = None
_watcher_widget: Optional[AssetWatcherWidget] = None


def register(session: ftrack_api.Session, **kw):
    """Register Asset Watcher plugin with ftrack Connect."""
    global _watcher_manager, _watcher_widget
    
    print("[AssetWatcher] register() called")
    logger.info("Registering Mroya Asset Watcher plugin...")
    
    # Create manager
    _watcher_manager = AssetWatcherManager(session)
    _watcher_manager.register()
    print("[AssetWatcher] Manager created and registered")
    
    # Add tab to Connect (delayed to ensure UI is ready)
    def _add_tab_when_ready():
        global _watcher_widget
        print("[AssetWatcher] _add_tab_when_ready() called")
        try:
            _ensure_qt()
            
            # Find the ftrack Connect main window through Qt
            app = QtWidgets.QApplication.instance()
            print(f"[AssetWatcher] QApplication = {app}")
            
            if not app:
                print("[AssetWatcher] No QApplication found")
                return
            
            # Find ftrack Connect window
            connect_app = None
            for widget in app.topLevelWidgets():
                print(f"[AssetWatcher] Top widget: {widget.__class__.__name__}")
                if hasattr(widget, 'tabPanel'):
                    connect_app = widget
                    break
            
            print(f"[AssetWatcher] connect_app = {connect_app}")
            
            if connect_app and hasattr(connect_app, 'tabPanel'):
                print(f"[AssetWatcher] tabPanel = {connect_app.tabPanel}")
                _watcher_widget = create_watcher_widget(session, _watcher_manager)
                print(f"[AssetWatcher] widget created = {_watcher_widget}")
                connect_app.tabPanel.addTab(_watcher_widget, "Asset Watcher")
                print("[AssetWatcher] Tab added successfully!")
                logger.info("Asset Watcher tab added to ftrack Connect")
            else:
                print("[AssetWatcher] WARNING: tabPanel not found or app not ready")
                logger.warning("Could not add tab - tabPanel not found or app not ready")
                
        except Exception as e:
            print(f"[AssetWatcher] ERROR: {e}")
            import traceback
            traceback.print_exc()
            logger.error(f"Failed to create Asset Watcher widget: {e}")
    
    # Use timer to delay tab creation until UI is ready
    try:
        _ensure_qt()
        print("[AssetWatcher] Scheduling tab creation in 1 second...")
        QtCore.QTimer.singleShot(1000, _add_tab_when_ready)
    except Exception as e:
        print(f"[AssetWatcher] Could not schedule: {e}")
        logger.warning(f"Could not schedule tab creation: {e}")
    
    print("[AssetWatcher] register() complete")
    logger.info("Mroya Asset Watcher plugin registered successfully")
