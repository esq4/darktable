#include "bauhaus/bauhaus.h"
#include "common/undo.h"
#include "develop/masks.h"
#include "common/selection.h"
#include "gui/accelerators.h"
#include "develop/blend.h"
#include "common/collection.h"
#include "common/overlay.h"

#include "common/darktable.h"


// из src/views/darkroom.c
#ifdef USE_LUA
static void _fire_darkroom_image_loaded_event(const bool clean, const dt_imgid_t imgid)
{
  dt_lua_async_call_alien(dt_lua_event_trigger_wrapper,
      0, NULL, NULL,
      LUA_ASYNC_TYPENAME, "const char*", "darkroom-image-loaded",
      LUA_ASYNC_TYPENAME, "bool", clean,
      LUA_ASYNC_TYPENAME, "dt_lua_image_t", GINT_TO_POINTER(imgid),
      LUA_ASYNC_DONE);
}
#endif

// _dev_load_requested_image из src/views/darkroom.c
static gboolean dt_next_img_dev_load_requested_image(gpointer user_data)
{
  dt_develop_t *dev = user_data;
  const dt_imgid_t imgid = dev->requested_id;

  if(dev->image_storage.id == NO_IMGID
     && dev->image_storage.id == imgid) return G_SOURCE_REMOVE;

  // make sure we can destroy and re-setup the pixel pipes.
  // we acquire the pipe locks, which will block the processing threads
  // in darkroom mode before they touch the pipes (init buffers etc).
  // we don't block here, since we hold the gdk lock, which will
  // result in circular locking when background threads emit signals
  // which in turn try to acquire the gdk lock.
  //
  // worst case, it'll drop some change image events. sorry.
  if(dt_pthread_mutex_BAD_trylock(&dev->preview_pipe->mutex))
  {

#ifdef USE_LUA

  _fire_darkroom_image_loaded_event(FALSE, imgid);

#endif
  return G_SOURCE_CONTINUE;
  }
  if(dt_pthread_mutex_BAD_trylock(&dev->full.pipe->mutex))
  {
    dt_pthread_mutex_BAD_unlock(&dev->preview_pipe->mutex);

 #ifdef USE_LUA

  _fire_darkroom_image_loaded_event(FALSE, imgid);

#endif

   return G_SOURCE_CONTINUE;
  }
  if(dt_pthread_mutex_BAD_trylock(&dev->preview2.pipe->mutex))
  {
    dt_pthread_mutex_BAD_unlock(&dev->full.pipe->mutex);
    dt_pthread_mutex_BAD_unlock(&dev->preview_pipe->mutex);

 #ifdef USE_LUA

  _fire_darkroom_image_loaded_event(FALSE, imgid);

#endif

   return G_SOURCE_CONTINUE;
  }

  const dt_imgid_t old_imgid = dev->image_storage.id;

  dt_overlay_add_from_history(old_imgid);

  // be sure light table will update the thumbnail
  if(!dt_history_hash_is_mipmap_synced(old_imgid))
  {
    dt_mipmap_cache_remove(darktable.mipmap_cache, old_imgid);
    dt_image_update_final_size(old_imgid);
    dt_image_synch_xmp(old_imgid);
    dt_history_hash_set_mipmap(old_imgid);
#ifdef USE_LUA
    dt_lua_async_call_alien(dt_lua_event_trigger_wrapper,
        0, NULL, NULL,
        LUA_ASYNC_TYPENAME, "const char*", "darkroom-image-history-changed",
        LUA_ASYNC_TYPENAME, "dt_lua_image_t", GINT_TO_POINTER(old_imgid),
        LUA_ASYNC_DONE);
#endif
  }

  // clean the undo list
  dt_undo_clear(darktable.undo, DT_UNDO_DEVELOP);

  // cleanup visible masks
  if(!dev->form_gui)
  {
    dev->form_gui = (dt_masks_form_gui_t *)calloc(1, sizeof(dt_masks_form_gui_t));
    dt_masks_init_form_gui(dev->form_gui);
  }
  dt_masks_change_form_gui(NULL);

  while(dev->history)
  {
    // clear history of old image
    dt_dev_history_item_t *hist = dev->history->data;
    dt_dev_free_history_item(hist);
    dev->history = g_list_delete_link(dev->history, dev->history);
  }

  // get new image:
  dt_dev_reload_image(dev, imgid);

  // make sure no signals propagate here:
  ++darktable.gui->reset;

  dt_pthread_mutex_lock(&dev->history_mutex);
  dt_dev_pixelpipe_cleanup_nodes(dev->full.pipe);
  dt_dev_pixelpipe_cleanup_nodes(dev->preview_pipe);
  dt_dev_pixelpipe_cleanup_nodes(dev->preview2.pipe);

  // chroma data will be fixed by reading whitebalance data from history
  dt_dev_reset_chroma(dev);

  const guint nb_iop = g_list_length(dev->iop);
  for(int i = nb_iop - 1; i >= 0; i--)
  {
    dt_iop_module_t *module = (g_list_nth_data(dev->iop, i));

    // the base module is the one with the lowest multi_priority
    int base_multi_priority = 0;
    for(const GList *l = dev->iop; l; l = g_list_next(l))
    {
      dt_iop_module_t *mod = l->data;
      if(dt_iop_module_is(module->so, mod->op))
        base_multi_priority = MIN(base_multi_priority, mod->multi_priority);
    }

    if(module->multi_priority == base_multi_priority) // if the module is the "base" instance, we keep it
    {
      module->iop_order = dt_ioppr_get_iop_order(dev->iop_order_list, module->op, module->multi_priority);
      module->multi_priority = 0;
      module->multi_name[0] = '\0';
      dt_iop_reload_defaults(module);
    }
    else // else we delete it and remove it from the panel
    {
      if(!dt_iop_is_hidden(module))
      {
        dt_iop_gui_cleanup_module(module);
      }

      // we remove the module from the list
      dev->iop = g_list_remove_link(dev->iop, g_list_nth(dev->iop, i));

      // we cleanup the module
      dt_action_cleanup_instance_iop(module);

      free(module);
    }
  }
  dev->iop = g_list_sort(dev->iop, dt_sort_iop_by_order);

  // we also clear the saved modules
  while(dev->alliop)
  {
    dt_iop_cleanup_module((dt_iop_module_t *)dev->alliop->data);
    free(dev->alliop->data);
    dev->alliop = g_list_delete_link(dev->alliop, dev->alliop);
  }
  // and masks
  g_list_free_full(dev->forms, (void (*)(void *))dt_masks_free_form);
  dev->forms = NULL;
  g_list_free_full(dev->allforms, (void (*)(void *))dt_masks_free_form);
  dev->allforms = NULL;

  dt_dev_pixelpipe_create_nodes(dev->full.pipe, dev);
  dt_dev_pixelpipe_create_nodes(dev->preview_pipe, dev);
  if(dev->preview2.widget && GTK_IS_WIDGET(dev->preview2.widget))
    dt_dev_pixelpipe_create_nodes(dev->preview2.pipe, dev);
  dt_dev_read_history(dev);

  // we have to init all module instances other than "base" instance
  char option[1024];
  for(const GList *modules = g_list_last(dev->iop); modules; modules = g_list_previous(modules))
  {
    dt_iop_module_t *module = modules->data;
    if(module->multi_priority > 0)
    {
      if(!dt_iop_is_hidden(module))
      {
        dt_iop_gui_init(module);

        /* add module to right panel */
        dt_iop_gui_set_expander(module);
        dt_iop_gui_update_blending(module);
      }
    }
    else
    {
      //  update the module header to ensure proper multi-name display
      if(!dt_iop_is_hidden(module))
      {
        snprintf(option, sizeof(option), "plugins/darkroom/%s/expanded", module->op);
        module->expanded = dt_conf_get_bool(option);
        dt_iop_gui_update_expanded(module);
        if(module->change_image) module->change_image(module);
        dt_iop_gui_update_header(module);
      }
    }
  }

  dt_dev_pop_history_items(dev, dev->history_end);
  dt_pthread_mutex_unlock(&dev->history_mutex);

  // set the module list order
  dt_dev_reorder_gui_module_list(dev);

  /* cleanup histograms */
  g_list_foreach(dev->iop, (GFunc)dt_iop_cleanup_histogram, (gpointer)NULL);

  /* make signals work again, we can't restore the active_plugin while signals
     are blocked due to implementation of dt_iop_request_focus so we do it now
     A double history entry is not generated.
  */
  --darktable.gui->reset;

  dt_dev_masks_list_change(dev);

  /* Now we can request focus again and write a safe plugins/darkroom/active */
  const char *active_plugin = dt_conf_get_string_const("plugins/darkroom/active");
  if(active_plugin)
  {
    gboolean valid = FALSE;
    for(const GList *modules = dev->iop; modules; modules = g_list_next(modules))
    {
      dt_iop_module_t *module = modules->data;
      if(dt_iop_module_is(module->so, active_plugin))
      {
        valid = TRUE;
        dt_iop_request_focus(module);
      }
    }
    if(!valid)
    {
      dt_conf_set_string("plugins/darkroom/active", "");
    }
  }

  // Signal develop initialize
  DT_CONTROL_SIGNAL_RAISE(DT_SIGNAL_DEVELOP_IMAGE_CHANGED);

  // release pixel pipe mutices
  dt_pthread_mutex_BAD_unlock(&dev->preview2.pipe->mutex);
  dt_pthread_mutex_BAD_unlock(&dev->preview_pipe->mutex);
  dt_pthread_mutex_BAD_unlock(&dev->full.pipe->mutex);

  // update hint message
  dt_collection_hint_message(darktable.collection);

  // update accels_window
  darktable.view_manager->accels_window.prevent_refresh = FALSE;
  if(darktable.view_manager->accels_window.window && darktable.view_manager->accels_window.sticky)
    dt_view_accels_refresh(darktable.view_manager);

  // just make sure at this stage we have only history info into the undo, all automatic
  // tagging should be ignored.
  dt_undo_clear(darktable.undo, DT_UNDO_TAGS);

  //connect iop accelerators
  dt_iop_connect_accels_all();

  /* last set the group to update visibility of iop modules for new pipe */
  dt_dev_modulegroups_set(dev, dt_conf_get_int("plugins/darkroom/groups"));

  dt_image_check_camera_missing_sample(&dev->image_storage);

#ifdef USE_LUA

  _fire_darkroom_image_loaded_event(TRUE, imgid);

#endif

  return G_SOURCE_REMOVE;
}


// _dev_change_image из src/views/darkroom.c
static void dt_next_img_dev_change_image(dt_develop_t *dev, const dt_imgid_t imgid)
{
  // Pipe reset needed when changing image
  // FIXME: synch with dev_init() and dev_cleanup() instead of redoing it

  // change active image
  g_slist_free(darktable.view_manager->active_images);
  darktable.view_manager->active_images = g_slist_prepend(NULL, GINT_TO_POINTER(imgid));
  DT_CONTROL_SIGNAL_RAISE(DT_SIGNAL_ACTIVE_IMAGES_CHANGE);

  // if the previous shown image is selected and the selection is unique
  // then we change the selected image to the new one
  if(dt_is_valid_imgid(dev->requested_id))
  {
    sqlite3_stmt *stmt;
    // clang-format off
    DT_DEBUG_SQLITE3_PREPARE_V2
      (dt_database_get(darktable.db),
       "SELECT m.imgid"
       " FROM memory.collected_images as m, main.selected_images as s"
       " WHERE m.imgid=s.imgid",
       -1, &stmt, NULL);
    // clang-format on
    gboolean follow = FALSE;
    if(sqlite3_step(stmt) == SQLITE_ROW)
    {
      if(sqlite3_column_int(stmt, 0) == dev->requested_id
         && sqlite3_step(stmt) != SQLITE_ROW)
      {
        follow = TRUE;
      }
    }
    sqlite3_finalize(stmt);
    if(follow)
    {
      dt_selection_select_single(darktable.selection, imgid);
    }
  }

  // disable color picker when changing image
  if(darktable.lib->proxy.colorpicker.picker_proxy)
    dt_iop_color_picker_reset(darktable.lib->proxy.colorpicker.picker_proxy->module, FALSE);

  // update aspect ratio
  if(dev->preview_pipe->backbuf
     && dev->preview_pipe->status == DT_DEV_PIXELPIPE_VALID)
  {
    const double aspect_ratio =
      (double)dev->preview_pipe->backbuf_width / (double)dev->preview_pipe->backbuf_height;
    dt_image_set_aspect_ratio_to(dev->preview_pipe->image.id, aspect_ratio, TRUE);
  }
  else
  {
    dt_image_set_aspect_ratio(dev->image_storage.id, TRUE);
  }

  // prevent accels_window to refresh
  darktable.view_manager->accels_window.prevent_refresh = TRUE;

  // get current plugin in focus before defocus
  const dt_iop_module_t *gui_module = dt_dev_gui_module();
  if(gui_module)
  {
    dt_conf_set_string("plugins/darkroom/active",
                       gui_module->op);
  }

  // store last active group
  dt_conf_set_int("plugins/darkroom/groups", dt_dev_modulegroups_get(dev));

  // commit any pending changes in focused module
  dt_iop_request_focus(NULL);

  g_assert(dev->gui_attached);

  // commit image ops to db
  dt_dev_write_history(dev);

  dev->requested_id = imgid;
  dt_dev_clear_chroma_troubles(dev);

  // possible enable autosaving due to conf setting but wait for some seconds for first save
  darktable.develop->autosaving = (double)dt_conf_get_int("autosave_interval") > 1.0;
  darktable.develop->autosave_time = dt_get_wtime() + 10.0;

  g_idle_add(dt_next_img_dev_load_requested_image, dev);
}


