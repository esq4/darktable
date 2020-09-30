#include "bauhaus/bauhaus.h"
#include "common/undo.h"
#include "develop/masks.h"
#include "common/selection.h"
#include "gui/accelerators.h"
#include "develop/blend.h"
#include "common/collection.h"

//#include "dtgtk/thumbtable.h"
//#include "common/darktable.h"

// из src/views/darkroom.c
static void dr_dev_change_image(dt_develop_t *dev, const uint32_t imgid)
{
  // change active image
  g_slist_free(darktable.view_manager->active_images);
  darktable.view_manager->active_images = NULL;
  darktable.view_manager->active_images
      = g_slist_append(darktable.view_manager->active_images, GINT_TO_POINTER(imgid));
  DT_DEBUG_CONTROL_SIGNAL_RAISE(darktable.signals, DT_SIGNAL_ACTIVE_IMAGES_CHANGE);

  // if the previous shown image is selected and the selection is unique
  // then we change the selected image to the new one
  //
  if(dev->image_storage.id > 0)
  {
    sqlite3_stmt *stmt;
    DT_DEBUG_SQLITE3_PREPARE_V2(dt_database_get(darktable.db),
                                "SELECT m.imgid FROM memory.collected_images as m, main.selected_images as s "
                                "WHERE m.imgid=s.imgid",
                                -1, &stmt, NULL);
    gboolean follow = FALSE;
    if(sqlite3_step(stmt) == SQLITE_ROW)
    {
      if(sqlite3_column_int(stmt, 0) == dev->image_storage.id && sqlite3_step(stmt) != SQLITE_ROW)
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
  if(dev->gui_module)
  {
    dev->gui_module->request_color_pick = DT_REQUEST_COLORPICK_OFF;
  }

  // update aspect ratio
  if(dev->preview_pipe->backbuf && dev->preview_status == DT_DEV_PIXELPIPE_VALID)
  {
    double aspect_ratio = (double)dev->preview_pipe->backbuf_width / (double)dev->preview_pipe->backbuf_height;
    dt_image_set_aspect_ratio_to(dev->preview_pipe->image.id, aspect_ratio, TRUE);
  }
  else
  {
      dt_image_set_aspect_ratio(dev->image_storage.id, TRUE);
  }

  // clean the undo list
  dt_undo_clear(darktable.undo, DT_UNDO_DEVELOP);

  // prevent accels_window to refresh
  darktable.view_manager->accels_window.prevent_refresh = TRUE;

  // make sure we can destroy and re-setup the pixel pipes.
  // we acquire the pipe locks, which will block the processing threads
  // in darkroom mode before they touch the pipes (init buffers etc).
  // we don't block here, since we hold the gdk lock, which will
  // result in circular locking when background threads emit signals
  // which in turn try to acquire the gdk lock.
  //
  // worst case, it'll drop some change image events. sorry.
  if(dt_pthread_mutex_BAD_trylock(&dev->preview_pipe_mutex)) return;
  if(dt_pthread_mutex_BAD_trylock(&dev->pipe_mutex))
  {
    dt_pthread_mutex_BAD_unlock(&dev->preview_pipe_mutex);
    return;
  }
  if(dt_pthread_mutex_BAD_trylock(&dev->preview2_pipe_mutex))
  {
    dt_pthread_mutex_BAD_unlock(&dev->pipe_mutex);
    dt_pthread_mutex_BAD_unlock(&dev->preview_pipe_mutex);
    return;
  }

  // get current plugin in focus before defocus
  gchar *active_plugin = NULL;
  if(darktable.develop->gui_module)
  {
    active_plugin = g_strdup(darktable.develop->gui_module->op);
  }

  // store last active group
  dt_conf_set_int("plugins/darkroom/groups", dt_dev_modulegroups_get(dev));

  dt_iop_request_focus(NULL);

  //ab
  g_assert(dev->gui_attached);

  // commit image ops to db
  dt_dev_write_history(dev);

  // be sure light table will update the thumbnail
  if (!dt_history_hash_is_mipmap_synced(dev->image_storage.id))
  {
    dt_mipmap_cache_remove(darktable.mipmap_cache, dev->image_storage.id);
    dt_image_reset_final_size(dev->image_storage.id);
    dt_image_synch_xmp(dev->image_storage.id);
    dt_history_hash_set_mipmap(dev->image_storage.id);
  }

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
    dt_dev_history_item_t *hist = (dt_dev_history_item_t *)(dev->history->data);
    dt_dev_free_history_item(hist);
    dev->history = g_list_delete_link(dev->history, dev->history);
  }

  // get new image:
  dt_dev_reload_image(dev, imgid);

  // make sure no signals propagate here:
  ++darktable.gui->reset;

  const guint nb_iop = g_list_length(dev->iop);
  dt_dev_pixelpipe_cleanup_nodes(dev->pipe);
  dt_dev_pixelpipe_cleanup_nodes(dev->preview_pipe);
  dt_dev_pixelpipe_cleanup_nodes(dev->preview2_pipe);
  for(int i = nb_iop - 1; i >= 0; i--)
  {
    dt_iop_module_t *module = (dt_iop_module_t *)(g_list_nth_data(dev->iop, i));

    // the base module is the one with the lowest multi_priority
    const guint clen = g_list_length(dev->iop);
    int base_multi_priority = 0;
    for(int k = 0; k < clen; k++)
    {
      dt_iop_module_t *mod = (dt_iop_module_t *)(g_list_nth_data(dev->iop, k));
      if(strcmp(module->op, mod->op) == 0) base_multi_priority = MIN(base_multi_priority, mod->multi_priority);
    }

    if(module->multi_priority == base_multi_priority) // if the module is the "base" instance, we keep it
    {
      module->iop_order = dt_ioppr_get_iop_order(dev->iop_order_list, module->op, module->multi_priority);
      module->multi_priority = 0;
      module->multi_name[0] = '\0';
      dt_iop_reload_defaults(module);
      dt_iop_gui_update(module);
    }
    else // else we delete it and remove it from the panel
    {
      if(!dt_iop_is_hidden(module))
      {
        gtk_widget_destroy(module->expander);
        dt_iop_gui_cleanup_module(module);
      }

      // we remove the module from the list
      dev->iop = g_list_remove_link(dev->iop, g_list_nth(dev->iop, i));

      // we cleanup the module
      dt_accel_disconnect_list(&module->accel_closures);
      dt_accel_cleanup_locals_iop(module);
      dt_iop_cleanup_module(module);
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

  dt_dev_pixelpipe_create_nodes(dev->pipe, dev);
  dt_dev_pixelpipe_create_nodes(dev->preview_pipe, dev);
  if(dev->second_window.widget && GTK_IS_WIDGET(dev->second_window.widget))
    dt_dev_pixelpipe_create_nodes(dev->preview2_pipe, dev);
  dt_dev_read_history(dev);

  // we have to init all module instances other than "base" instance
  GList *modules = g_list_last(dev->iop);
  while(modules)
  {
    dt_iop_module_t *module = (dt_iop_module_t *)(modules->data);
    if(module->multi_priority > 0)
    {
      if(!dt_iop_is_hidden(module))
      {
        module->gui_init(module);
        dt_iop_reload_defaults(module);

        /* add module to right panel */
        GtkWidget *expander = dt_iop_gui_get_expander(module);
        dt_ui_container_add_widget(darktable.gui->ui, DT_UI_CONTAINER_PANEL_RIGHT_CENTER, expander);
        dt_iop_gui_set_expanded(module, FALSE, dt_conf_get_bool("darkroom/ui/single_module"));
        dt_iop_gui_update_blending(module);
      }
    }
    else
    {
      //  update the module header to ensure proper multi-name display
      if(!dt_iop_is_hidden(module))
      {
        if(module->change_image) module->change_image(module);
        dt_iop_gui_update_header(module);
      }
    }

    modules = g_list_previous(modules);
  }

  dt_dev_pop_history_items(dev, dev->history_end);

  // set the module list order
  dt_dev_reorder_gui_module_list(dev);

  dt_dev_masks_list_change(dev);

  /* last set the group to update visibility of iop modules for new pipe */
  dt_dev_modulegroups_set(dev, dt_conf_get_int("plugins/darkroom/groups"));

  /* cleanup histograms */
  g_list_foreach(dev->iop, (GFunc)dt_iop_cleanup_histogram, (gpointer)NULL);

  /* make signals work again, we can't restore the active_plugin while signals
     are blocked due to implementation of dt_iop_request_focus so we do it now
     A double history entry is not generated.
  */
  --darktable.gui->reset;

  /* Now we can request focus again and write a safe plugins/darkroom/active */
  if(active_plugin)
  {
    gboolean valid = FALSE;
    modules = dev->iop;
    while(modules)
    {
      dt_iop_module_t *module = (dt_iop_module_t *)(modules->data);
      if(!strcmp(module->op, active_plugin))
      {
        valid = TRUE;
        dt_conf_set_string("plugins/darkroom/active", active_plugin);
        dt_iop_request_focus(module);
      }
      modules = g_list_next(modules);
    }
    if(!valid)
    {
      dt_conf_set_string("plugins/darkroom/active", "");
    }
    g_free(active_plugin);
  }

  // Signal develop initialize
  DT_DEBUG_CONTROL_SIGNAL_RAISE(darktable.signals, DT_SIGNAL_DEVELOP_IMAGE_CHANGED);

  // release pixel pipe mutices
  dt_pthread_mutex_BAD_unlock(&dev->preview2_pipe_mutex);
  dt_pthread_mutex_BAD_unlock(&dev->preview_pipe_mutex);
  dt_pthread_mutex_BAD_unlock(&dev->pipe_mutex);

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


  // just make sure at this stage we have only history info into the undo, all automatic
  // tagging should be ignored.
  dt_undo_clear(darktable.undo, DT_UNDO_TAGS);
}

// set offset and redraw if needed
//gboolean dr_thumbtable_set_offset(dt_thumbtable_t *table, const int offset, const gboolean redraw)
//{
//  if(offset < 1 || offset == table->offset) return FALSE;
//  table->offset = offset;
//  dt_conf_set_int("plugins/lighttable/recentcollect/pos0", table->offset);
//  if(redraw) dt_thumbtable_full_redraw(table, TRUE);
//  return TRUE;
//}

//gboolean dr_event_rating_release(dt_develop_t *user_data)
//{
////  dt_thumbnail_t *thumb = (dt_thumbnail_t *)user_data;
////  const int imgid = thumb->imgid;
//  const int imgid = 1;
//  int new_offset = 1;
//  int new_id = -1;
//  int diff = 1;
//  new_offset = 1;

//  dt_develop_t *dev = (dt_develop_t *)user_data;

//  // we new offset and imgid after the jump
//  sqlite3_stmt *stmt;
//  gchar *query = dt_util_dstrcat(NULL, "SELECT rowid, imgid "
//                                          "FROM memory.collected_images "
//                                          "WHERE rowid=(SELECT rowid FROM memory.collected_images WHERE imgid=%d)+%d",
//                                 imgid, diff);
//  DT_DEBUG_SQLITE3_PREPARE_V2(dt_database_get(darktable.db), query, -1, &stmt, NULL);
//  if(sqlite3_step(stmt) == SQLITE_ROW)
//  {
//    new_offset = sqlite3_column_int(stmt, 0);
//    new_id = sqlite3_column_int(stmt, 1);
//  }
//  else
//  {
//    // if we are here, that means that the current is not anymore in the list
//    // in this case, let's use the current offset image
////    new_id = dt_ui_thumbtable(darktable.gui->ui)->offset_imgid;
////    new_offset = dt_ui_thumbtable(darktable.gui->ui)->offset;
//  }
//  g_free(query);
//  sqlite3_finalize(stmt);

//  if(new_id < 0 || new_id == imgid) return FALSE;
//  dr_dev_change_image(dev, new_id);
//  //dr_thumbtable_set_offset(dt_ui_thumbtable(darktable.gui->ui), new_offset, TRUE);
////  dr_thumbtable_set_offset(dt_ui_thumbtable(darktable.gui->ui), new_offset, FALSE);
//  return TRUE;

//}
